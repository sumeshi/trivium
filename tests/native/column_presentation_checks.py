"""Native column arrangement and datetime presentation preserve source evidence."""
from __future__ import annotations

import csv
import os
from pathlib import Path
import time
import uuid

from column_pin_checks import _key, _menu_items, _prefix, _saved
from feedback_checks import _horizontal_to, _settle
from row_navigation import open_project, wait_for


def _action(web, column: str, command: str) -> None:
    header = web.script("return document.querySelector('.data-header [data-column-id=\"'+arguments[0]+'\"] .column-sort');", column)
    assert header, column
    web.script("arguments[0].focus({preventScroll:true});", header)
    _key(web, "\ue015")
    items = _menu_items(web)
    if command in ["Show ISO datetime", "Show original values"]:
        assert items[-1] == command, items
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


def _native_drag(web, start: dict, end: dict) -> None:
    # WebKitWebDriver does not reliably complete HTML5 dragging. As with the
    # native context-click check, XTest delivers actual OS button input.
    from ctypes import CDLL, c_char_p, c_int, c_ulong, c_void_p
    x11, xtest = CDLL("libX11.so.6"), CDLL("libXtst.so.6")
    x11.XOpenDisplay.argtypes, x11.XOpenDisplay.restype = [c_char_p], c_void_p
    x11.XSync.argtypes, x11.XCloseDisplay.argtypes = [c_void_p, c_int], [c_void_p]
    xtest.XTestFakeMotionEvent.argtypes = [c_void_p, c_int, c_int, c_int, c_ulong]
    xtest.XTestFakeButtonEvent.argtypes = [c_void_p, c_int, c_int, c_ulong]
    display = x11.XOpenDisplay(None)
    assert display, "Native column dragging requires the owned Xvfb display"
    rect = web.request("GET", web.route("window/rect"))
    try:
        assert xtest.XTestFakeMotionEvent(display, -1, rect["x"] + start["x"], rect["y"] + start["y"], 0)
        assert xtest.XTestFakeButtonEvent(display, 1, 1, 0)
        x11.XSync(display, 0)
        time.sleep(.1)
        for step in range(1, 13):
            x = rect["x"] + round(start["x"] + (end["x"] - start["x"]) * step / 12)
            y = rect["y"] + round(start["y"] + (end["y"] - start["y"]) * step / 12)
            assert xtest.XTestFakeMotionEvent(display, -1, x, y, 0)
            x11.XSync(display, 0)
            time.sleep(.025)
        time.sleep(.15)  # Match the Windows pointer's final-target dwell before releasing.
    finally:
        xtest.XTestFakeButtonEvent(display, 1, 0, 0)
        x11.XSync(display, 0)
        x11.XCloseDisplay(display)


def _drag(web, source: str, target: str, after: bool = False) -> dict:
    _horizontal_to(web, 0)
    # Match the full heading's midpoint, while dropping on its sort button rather than the actions button.
    points = web.script("""
      const a=document.querySelector('.data-header [data-column-id="'+arguments[0]+'"] .column-sort'),
        b=document.querySelector('.data-header [data-column-id="'+arguments[1]+'"]'),
        sort=b.querySelector('.column-sort'),r=a.getBoundingClientRect(),t=b.getBoundingClientRect(),s=sort.getBoundingClientRect();
      const start={x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height/2)},
        end={x:arguments[2] ? Math.floor(s.right-2) : Math.round(t.left+t.width/4),y:Math.round(t.top+t.height/2)};
      return {start,end,midpoint:t.left+t.width/2,
        reachable:a.contains(document.elementFromPoint(start.x,start.y))&&sort.contains(document.elementFromPoint(end.x,end.y))};
    """, source, target, after)
    assert points["reachable"], points
    assert (points["end"]["x"] >= points["midpoint"]) == after, points
    web.script("""
      window.__columnDrag=[];window.__columnDragOver=null;
      window.__columnDragAudit=e=>{
        const h=e.target.closest?.('.column-heading'),r=h?.getBoundingClientRect();
        const item={type:e.type,trusted:e.isTrusted,columnId:h?.dataset.columnId??null,x:e.clientX,midpoint:r?r.left+r.width/2:null};
        if(e.type==='dragover')window.__columnDragOver=item;else window.__columnDrag.push(item);
      };
      for(const name of ['dragstart','dragover','drop','dragend'])document.addEventListener(name,window.__columnDragAudit,true);
    """)
    start, end = points["start"], points["end"]
    try:
        if os.name != "nt":
            _native_drag(web, start, end)
            method = "XTest native drag"
        else:
            web.request("POST", web.route("actions"), {"actions": [{"type": "pointer", "id": "presentation-drag",
                "parameters": {"pointerType": "mouse"}, "actions": [
                    {"type": "pointerMove", **start, "origin": "viewport", "duration": 0},
                    {"type": "pointerDown", "button": 0},
                    {"type": "pointerMove", "x": start["x"] - 12, "y": start["y"], "origin": "viewport", "duration": 150},
                    {"type": "pointerMove", **end, "origin": "viewport", "duration": 350},
                    {"type": "pause", "duration": 150}, {"type": "pointerUp", "button": 0},
                ]}]})
            method = "WebDriver pointer"
        _settle(web)
        events = web.script("return window.__columnDrag;")
        return {"events": events, "last_over": web.script("return window.__columnDragOver;"),
                "delivered": any(event["type"] == "drop" and event["trusted"] for event in events), "input": method}
    finally:
        web.request("DELETE", web.route("actions"))
        web.script("for(const name of ['dragstart','dragover','drop','dragend'])document.removeEventListener(name,window.__columnDragAudit,true);delete window.__columnDragAudit;delete window.__columnDrag;delete window.__columnDragOver;")


def _reorder(web, source: str, target: str, expected: list[str], after: bool = False) -> dict:
    drag = _drag(web, source, target, after)
    assert drag["delivered"], f"Native column reordering requires an actual trusted HTML5 drop: {drag}"
    wait_for(lambda: _order(web), lambda order: order == expected, "the trusted drag changes the display order")
    return drag


def _focus_resize(web, column: str) -> None:
    web.script("document.querySelector('.data-header [data-column-id=\"'+arguments[0]+'\"] .column-sort').focus({preventScroll:true});", column)
    _key(web, "\ue032")  # F2 enters the same resize handle used by pointer dragging.
    wait_for(lambda: web.script("return document.activeElement.classList.contains('column-resize-handle')&&document.activeElement.closest('[data-column-id]')?.dataset.columnId;"),
             lambda value: value == column, "F2 focuses the column's resize handle")


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

        moved = [columns[name] for name in ["record", "message", "created", "not_time"]]
        drag = _reorder(web, columns["message"], columns["created"], moved)
        _unchanged(web, query)
        desired = [columns[name] for name in ["created", "record", "message", "not_time"]]
        _reorder(web, columns["created"], columns["record"], desired)
        _reorder(web, columns["created"], columns["message"], moved, after=True)
        _reorder(web, columns["created"], columns["record"], desired)

        initial_width = _width(web, columns["created"])["header"]
        handle = web.script("const e=document.querySelector('[aria-label=\"Resize column created\"]'),r=e.getBoundingClientRect();return {x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height/2),hit:e.contains(document.elementFromPoint(r.left+r.width/2,r.top+r.height/2))};")
        assert handle.pop("hit"), handle
        web.request("POST", web.route("actions"), {"actions": [{"type": "pointer", "id": "presentation-resize",
            "parameters": {"pointerType": "mouse"}, "actions": [
                {"type": "pointerMove", **handle, "origin": "viewport", "duration": 0}, {"type": "pointerDown", "button": 0},
                {"type": "pointerMove", "x": handle["x"] + 64, "y": handle["y"], "origin": "viewport", "duration": 200},
                {"type": "pointerUp", "button": 0}]}]})
        wait_for(lambda: _width(web, columns["created"]), lambda value: abs(value["header"] - initial_width - 64) <= 1, "drag resizes the source column")
        _focus_resize(web, columns["created"])
        _key(web, "\ue014")
        final_width = initial_width + 74
        widths = wait_for(lambda: _width(web, columns["created"]),
            lambda value: all(abs(width - final_width) <= 1 for width in value.values()), "header and body share keyboard-adjusted column width")
        _key(web, "\ue00c")
        wait_for(lambda: web.script("return document.activeElement.classList.contains('column-sort')&&document.activeElement.closest('[data-column-id]')?.dataset.columnId;"),
                 lambda value: value == columns["created"], "Escape returns from resizing to the same header")
        _focus_resize(web, columns["created"])
        _key(web, "\ue004")
        wait_for(lambda: web.script("return document.activeElement.closest('[data-column-id]')?.dataset.columnId;"),
                 lambda value: value == columns["record"], "Tab leaves resizing for the next logical header")
        _focus_resize(web, columns["record"])
        web.request("POST", web.route("actions"), {"actions": [{"type": "key", "id": "presentation-resize-back", "actions": [
            {"type": "keyDown", "value": "\ue008"}, {"type": "keyDown", "value": "\ue004"},
            {"type": "keyUp", "value": "\ue004"}, {"type": "keyUp", "value": "\ue008"}]}]})
        wait_for(lambda: web.script("return document.activeElement.closest('[data-column-id]')?.dataset.columnId;"),
                 lambda value: value == columns["created"], "Shift+Tab leaves resizing for the previous logical header")
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
            "six_item_menu_and_single_toggles": True, "keyboard_resize_f2_escape_tab": True,
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
