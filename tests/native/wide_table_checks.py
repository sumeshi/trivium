"""Bound wide-table DOM while preserving logical keyboard and row operations."""
from __future__ import annotations

import csv
from pathlib import Path
import uuid

from feedback_checks import _settle
from row_navigation import open_project, wait_for
from recycled_input_checks import verify_recycled_inputs


def _keys(web, key: str, count: int) -> None:
    web.request("POST", web.route("actions"), {"actions": [{"type": "key", "id": "wide-table-keys", "actions": [
        action for _ in range(count) for action in ({"type": "keyDown", "value": key}, {"type": "keyUp", "value": key})]}]})


def _shift_tabs(web, count: int) -> None:
    web.request("POST", web.route("actions"), {"actions": [{"type": "key", "id": "wide-table-keys", "actions": [{"type": "keyDown", "value": "\ue008"}]}]})
    try:
        _keys(web, "\ue004", count)
    finally:
        web.request("POST", web.route("actions"), {"actions": [{"type": "key", "id": "wide-table-keys", "actions": [{"type": "keyUp", "value": "\ue008"}]}]})


def _wheel(web, delta: int) -> None:
    point = web.script("const r=document.querySelector('.virtual-viewport').getBoundingClientRect();return {x:Math.round(r.left+r.width*.7),y:Math.round(r.top+80)};")
    previous = web.state()["scrollTop"]
    web.request("POST", web.route("actions"), {"actions": [{"type": "wheel", "id": f"wide-table-wheel-{uuid.uuid4().hex}", "actions": [
        {"type": "scroll", **point, "deltaX": 0, "deltaY": delta, "duration": 0}]}]})
    wait_for(web.state, lambda value: abs(value["scrollTop"] - previous - delta) <= 1, "trusted wheel advances the wide table")
    _settle(web)


def _horizontal_wheel(web, target: int) -> None:
    left = web.script("return document.querySelector('.virtual-viewport').scrollLeft;")
    point = web.script("const r=document.querySelector('.virtual-viewport').getBoundingClientRect();return {x:Math.round(r.left+r.width*.7),y:Math.round(r.top+80)};")
    web.request("POST", web.route("actions"), {"actions": [{"type": "wheel", "id": f"wide-horizontal-{uuid.uuid4().hex}", "actions": [
        {"type": "scroll", **point, "deltaX": target - left, "deltaY": 0, "duration": 0}]}]})
    wait_for(lambda: web.script("return document.querySelector('.virtual-viewport').scrollLeft;"),
        lambda value: abs(value - target) <= 1, "horizontal wheel reaches its requested offset without changing keyboard focus")
    _settle(web)
    assert abs(web.script("return document.querySelector('.virtual-viewport').scrollLeft;") - target) <= 1, "The footer must not feed the previous stored horizontal offset back after the frame"


def _geometry(web) -> dict:
    return web.script("""
      const viewport=document.querySelector('.virtual-viewport'),v=viewport.getBoundingClientRect(),
        headers=[...document.querySelectorAll('.data-header [data-column-id]')],rows=[...document.querySelectorAll('.data-row')];
      return {scrollLeft:viewport.scrollLeft,viewport:{left:v.left,right:v.right},rows:rows.length,
        headers:headers.map(e=>({id:e.dataset.columnId,left:e.getBoundingClientRect().left,right:e.getBoundingClientRect().right,frozen:e.dataset.frozen==='true'})),
        cellCounts:rows.map(e=>e.querySelectorAll(':scope>[data-column-id]').length),
        sourceColumns:rows.map(e=>[...e.querySelectorAll(':scope>[data-column-id]')].map(c=>c.dataset.columnId))};
    """)


def verify_wide_table(web, data, root: Path, evidence: Path, report: dict) -> None:
    total = 2500
    source = root / f"wide-table-{uuid.uuid4().hex[:8]}.csv"
    names = [f"field_{index:03d}" for index in range(1, 121)]
    with source.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(names)
        writer.writerows([f"r{row:04d}-c{column:03d}" for column in range(1, 121)] for row in range(total))
    case = web.command("create_project", {"path": str(source)})
    project_id = case["meta"]["id"]
    try:
        open_project(web, case["meta"]["name"])
        web.results(total)
        detail = web.command("load_project", {"projectId": project_id}, request=True)
        columns = {column["label"]: column["id"] for column in detail["columns_meta"]}
        first = _geometry(web)
        assert len(columns) == 120 and len(first["headers"]) < 16, first
        assert first["rows"] > 0 and max(first["cellCounts"]) < 16, first
        assert all(ids == [header["id"] for header in first["headers"]] for ids in first["sourceColumns"]), first
        assert [header["id"] for header in first["headers"] if header["frozen"]] == [columns[names[0]]]
        web.named(f"Actions for column {names[0]}")
        web.named("Unfreeze data columns", ".column-context-menu")
        wait_for(lambda: web.script("return document.querySelectorAll('.data-header .stickyData').length;"),
            lambda count: count == 0, "wide keyboard checks use no frozen data prefix")

        web.script("document.querySelector('.data-header [aria-label=\"Select visible rows\"]').focus();")
        _keys(web, "\ue004", 1)
        _keys(web, "\ue004", 59)
        wait_for(lambda: web.script("return document.activeElement.closest('[data-column-id]')?.dataset.columnId;"),
            lambda value: value == columns[names[59]], "all middle-header Tab inputs finish before an independent wheel gesture")
        _settle(web)
        web.script("window.__wideFocusedHeader=document.activeElement;")
        _horizontal_wheel(web, 0)
        assert web.script("return document.activeElement===window.__wideFocusedHeader&&document.activeElement.closest('[data-column-id]')?.dataset.columnId;" ) == columns[names[59]], "A focused offscreen header must retain its original DOM identity"
        _keys(web, "\ue004", 1)
        wait_for(lambda: web.script("return document.activeElement.closest('[data-column-id]')?.dataset.columnId;"),
            lambda value: value == columns[names[60]], "Tab resumes at the logical column after a horizontal wheel")
        web.script("document.querySelector('.data-header [aria-label=\"Select visible rows\"]').focus();")
        _keys(web, "\ue004", 1)
        _keys(web, "\ue004", 119)
        wait_for(lambda: web.script("return document.activeElement.closest('[data-column-id]')?.dataset.columnId;"),
            lambda value: value == columns[names[-1]], "119 real Tabs reach the last of120 configured columns")
        _settle(web)
        last = _geometry(web)
        target = next(header for header in last["headers"] if header["id"] == columns[names[-1]])
        assert target["left"] >= last["viewport"]["left"] + 450 and target["right"] <= last["viewport"]["right"] + 1, last
        assert len(last["headers"]) < 16 and max(last["cellCounts"]) < 16, last
        web.script("document.querySelector('.data-row .memo-button').focus();")
        _keys(web, "\ue004", 1)
        wait_for(lambda: web.script("return !!document.activeElement.closest('.data-row')&&document.activeElement.dataset.columnId;"),
            lambda value: value == columns[names[0]], "Tab from Note reveals the first logical data column after horizontal scrolling")
        web.script("document.querySelector('.data-row .select-cell input').focus();")
        _shift_tabs(web, 1)
        wait_for(lambda: web.script("return !!document.activeElement.closest('.data-header')&&document.activeElement.closest('[data-column-id]')?.dataset.columnId;"),
            lambda value: value == columns[names[-1]], "Shift+Tab from the first row reveals the final logical header")
        _shift_tabs(web, 119)
        wait_for(lambda: web.script("return document.activeElement.closest('[data-column-id]')?.dataset.columnId;"),
            lambda value: value == columns[names[0]], "Shift+Tab restores the first column without skipping unmounted headers")
        web.button("First")
        _wheel(web, 256)
        web.script("""
          const retained=[...document.querySelectorAll('.data-row')].filter(e=>Number(e.dataset.position)>=10&&Number(e.dataset.position)<=15);
          window.__wideRetention={nodes:new Map(retained.map(e=>[e.dataset.position,e])),mutations:[]};
          window.__wideRetention.observer=new MutationObserver(records=>{
            for(const r of records){const e=(r.target.nodeType===1?r.target:r.target.parentElement)?.closest('.data-row');
              if(e&&window.__wideRetention.nodes.has(e.dataset.position))window.__wideRetention.mutations.push({position:e.dataset.position,type:r.type,attribute:r.attributeName});}
          });
          window.__wideRetention.observer.observe(document.querySelector('.virtual-inner'),{subtree:true,attributes:true,childList:true,characterData:true});
        """)
        _wheel(web, 32)
        wait_for(lambda: web.script("return Number(document.querySelector('.data-row').dataset.position);"), lambda value: value == 1,
            "one-row virtual-window shift is rendered")
        _settle(web)
        retained = web.script("""
          const state=window.__wideRetention;state.observer.disconnect();
          return {rows:state.nodes.size,unchanged:[...state.nodes].every(([id,node])=>node.isConnected&&document.querySelector('.data-row[data-position="'+id+'"]')===node),mutations:state.mutations};
        """)
        assert retained["rows"] == 6 and retained["unchanged"] and not retained["mutations"], retained
        web.button("First")
        web.results(total)
        web.tap(web.node('.data-row .flag-safe'))
        wait_for(lambda: web.script("return !!document.querySelector('.data-row .flag-safe.selected');"), bool, "wide-table flag state remains live")
        web.tap(web.node('.data-row .memo-button'))
        web.write('textarea[aria-label="Note"]', "Wide-table note")
        web.named("Save", ".memo-dialog")
        wait_for(lambda: web.script("return document.querySelector('.data-row .memo-text')?.textContent;"),
            lambda value: value == "Wide-table note", "immutable rows receive completed note updates")
        wait_for(lambda: web.script("return !document.querySelector('.memo-dialog');"), bool,
            "the note write completes and its modal closes before further table input")
        _settle(web)
        saved = web.command("query_project_rows", {"projectId": project_id, "offset": 0, "limit": 1})["rows"][0]
        assert saved["row_index"] == 0 and saved["flag"] == "safe" and saved["memo"] == "Wide-table note", saved

        web.script("window.__wideRecycledControl=document.querySelector('.data-row .flag-critical');window.__wideRecycledControl.focus({preventScroll:true});")
        _wheel(web, 288)
        recycled_focus = web.script("const e=window.__wideRecycledControl;return {connected:e.isConnected,original:Number(e.closest('.data-row').dataset.rowIndex),active:document.activeElement===e};")
        assert recycled_focus["connected"] and recycled_focus["original"] != 0 and not recycled_focus["active"], recycled_focus

        # Hold a real previous-page response while the next page's first row is
        # already visible. Shift+Tab must skip its disabled loading controls.
        web.script("window.__workspaceHold={name:'query_project_rows',projectId:arguments[0],offset:1250};", project_id)
        try:
            web.input('.row-jump input', "1501", enter=True)
            wait_for(lambda: web.script("return !!window.__workspaceHold?.ready;"), bool, "the real previous-page response is held")
            wait_for(lambda: web.script("return !!document.querySelector('.data-row[data-position=\"1500\"] .select-cell input');"),
                bool, "the next page's first loaded row is visible beside loading placeholders")
            assert web.script("return !!document.querySelector('.data-row[data-position=\"1499\"].loading');")
            web.script("document.querySelector('.data-row[data-position=\"1500\"] .select-cell input').focus({preventScroll:true});")
            _shift_tabs(web, 1)
            wait_for(lambda: web.script("return !!document.activeElement.closest('.data-header')&&document.activeElement.closest('[data-column-id]')?.dataset.columnId;"),
                lambda value: value == columns[names[-1]], "Shift+Tab skips the held loading page and reaches the last header")
        finally:
            web.script("window.__workspaceHold?.release?.();delete window.__workspaceHold;")
        web.results(total)
        _horizontal_wheel(web, 0)
        web.input('.row-jump input', "100", enter=True)
        wait_for(lambda: web.script("return document.querySelector('.virtual-viewport').scrollTop;"),
            lambda value: value > 0, "the cross-axis check begins below the first row")
        web.script("""
          document.querySelector('.virtual-viewport').addEventListener('wheel',()=>{
            const v=document.querySelector('.virtual-viewport');
            window.__wideCrossAxis={before:{top:v.scrollTop,left:v.scrollLeft}};
            // Trigger the real First handler before this trusted wheel's rAF,
            // deterministically exercising both axes in one event task.
            document.querySelector('.row-jump [aria-label="First"]').click();
            window.__wideCrossAxis.after={top:v.scrollTop,left:v.scrollLeft};
          },{once:true});
        """)
        _horizontal_wheel(web, 700)
        cross_axis = web.script("return window.__wideCrossAxis;")
        assert cross_axis["before"]["top"] > 0 and cross_axis["after"]["top"] == 0, cross_axis
        assert cross_axis["before"]["left"] == cross_axis["after"]["left"] == 700, cross_axis
        verify_recycled_inputs(web, project_id, report)
        web.screenshot(evidence / "wide-table-bounded-dom.png")
        report["wide_table"] = {"configured_columns": 120, "rows": total, "initial": first, "last_column": last,
            "retained_rows": retained, "recycled_focus": recycled_focus, "cross_axis_navigation": cross_axis, "header_tabs": 119,
            "focused_column_survives_wheel": True, "loading_page_shift_tab": True, "flag_and_note_live": True}
    finally:
        web.script("window.__workspaceHold?.release?.();delete window.__workspaceHold;window.__wideRetention?.observer.disconnect();delete window.__wideRetention;delete window.__wideFocusedHeader;delete window.__wideRecycledControl;delete window.__wideCrossAxis;")
        if web.script("return !!document.querySelector('.memo-dialog');"):
            web.named("Cancel", ".memo-dialog")
        web.close_drawer()
        web.tab(data["id"])
        web.results(data["total"])
        web.named(f"Close {source.name}")
        web.command("delete_project", {"projectId": project_id}, request=True)
        open_project(web, data["name"])
        web.results(data["total"])
