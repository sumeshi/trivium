"""Native row-detail and sparse timeline regressions for investigation refinements."""
from __future__ import annotations

import json
from pathlib import Path
import uuid

from feedback_checks import _clear, _settle
from row_navigation import open_project, wait_for


def _field(web, column_id: str):
    return web.node(f'.row-detail-field[data-column-id="{column_id}"]')


def _open_detail(web) -> None:
    web.click(web.script("return document.querySelector('.data-row .stickyData');"))
    wait_for(lambda: web.script("return !!document.querySelector('.row-detail-dialog');"), bool, "actual row detail opens")


def _copy_audit(web) -> None:
    assert web.script("""
      const clip=navigator.clipboard,write=clip?.writeText,exec=document.execCommand;
      window.__detailCopyAudit={values:[],restore(){if(clip&&write)Object.defineProperty(clip,'writeText',{value:write,configurable:true,writable:true});document.execCommand=exec;}};
      if(clip&&write)Object.defineProperty(clip,'writeText',{configurable:true,writable:true,value:function(text){window.__detailCopyAudit.values.push(text);return write.call(clip,text);}});
      document.execCommand=function(command,...args){if(command==='copy')window.__detailCopyAudit.values.push(document.activeElement?.value??[...document.querySelectorAll('textarea')].at(-1)?.value??getSelection().toString());return exec.call(document,command,...args);};
      return true;
    """), "Copy audit must forward to the actual clipboard implementation"


def _details(web, project_id: str, columns: dict[str, str], message: str, total: int, evidence: Path) -> dict:
    checkpoint = len(web.trace())
    _open_detail(web)
    wait_for(lambda: web.script("return document.querySelector(arguments[0])?.querySelector('.row-field-value')?.textContent;", f'.row-detail-field[data-column-id="{columns["message"]}"]'), lambda value: value == message, "truncated message is fetched and shown in full")
    assert web.script("return document.querySelector('.row-detail-dialog').getAttribute('aria-label');") == "Row 1"
    assert not web.script("return [...document.querySelectorAll('.row-detail-dialog button,.row-detail-dialog select,.row-detail-dialog label')].some(e=>/around|neighbor|surround/i.test(e.textContent));")
    fields = web.script("return [...document.querySelectorAll('.row-detail-field')].map(e=>{const s=getComputedStyle(e);return {column:e.dataset.columnId,borders:[s.borderTopWidth,s.borderRightWidth,s.borderBottomWidth,s.borderLeftWidth].map(parseFloat)};});")
    assert len(fields) == len(columns) and all(any(width > 0 for width in item["borders"]) for item in fields), fields
    assert web.script("return arguments[0].querySelector('.row-field-value').textContent;", _field(web, columns["huge"])) == "18446744073709551615"
    assert web.script("return arguments[0].querySelector('button[aria-label^=\"Filter in \"]').disabled;", _field(web, columns["nullable"]))
    _copy_audit(web)
    try:
        for label, expected in (("message", message), ("huge", "18446744073709551615")):
            before = len(web.script("return window.__detailCopyAudit.values;"))
            available = web.script("return [...document.querySelectorAll('.row-detail-dialog button')].map(e=>({label:e.getAttribute('aria-label'),text:e.textContent.trim(),disabled:e.disabled}));")
            assert any(button['label'] == f'Copy {label}' and not button['disabled'] for button in available), available
            if label == 'huge':
                button = web.node(f'.row-detail-dialog [aria-label="Copy {label}"]')
                web.script("arguments[0].focus();", button)
                web.request("POST", web.route("actions"), {"actions": [{"type": "key", "id": "detail-copy-keyboard", "actions": [
                    {"type": "keyDown", "value": "\ue007"}, {"type": "keyUp", "value": "\ue007"}]}]})
            else:
                web.named(f"Copy {label}", ".row-detail-dialog")
            wait_for(lambda: web.script("return document.querySelector('.row-detail-copy-status')?.textContent;"), lambda value: bool(value and "Copied" in value and label in value), f"{label} reaches the real clipboard")
            assert web.script("return !!document.querySelector('.row-detail-dialog');"), 'Copy must keep the row dialog open'
            copied = web.script("return window.__detailCopyAudit.values.slice(arguments[0]);", before)
            assert copied and all(value == expected for value in copied), (label, copied)
        web.screenshot(evidence / "followup-row-detail.png")
    finally:
        web.script("window.__detailCopyAudit?.restore();delete window.__detailCopyAudit;")
    web.named("Filter in message", ".row-detail-dialog")
    web.applied(projectId=project_id, condition={"column": columns["message"], "op": "eq", "values": [message], "negated": False})
    web.results(1)
    assert not web.script("return !!document.querySelector('.row-detail-dialog');")
    _clear(web, total)
    _open_detail(web)
    web.named("Filter in empty", ".row-detail-dialog")
    web.applied(projectId=project_id, condition={"column": columns["empty"], "op": "eq", "values": [""], "negated": False})
    web.results(1)
    _clear(web, total)
    _open_detail(web)
    web.named("Filter out message", ".row-detail-dialog")
    web.applied(projectId=project_id, condition={"column": columns["message"], "op": "ne", "values": [message], "negated": False})
    web.results(total - 1)
    _clear(web, total)
    requests = web.trace()[checkpoint:]
    assert not any(entry["name"] == "surrounding_rows" for entry in requests), requests
    full_reads = [entry for entry in requests if entry["name"] == "get_cell" and entry["request"].get("columnId") == columns["message"]]
    assert full_reads and all(entry["requestId"] for entry in full_reads), full_reads
    return {"fields_have_borders": True, "full_long_value_and_integer_copy": True,
        "clipboard_audit": "Payload recorded while forwarding to the actual clipboard implementation",
        "exact_in_out_filters": True, "empty_string_filter": True, "nullable_filter_disabled": True,
        "surrounding_rows_requests": 0, "owned_full_cell_reads": len(full_reads)}


def _timeline(web, project_id: str, evidence: Path) -> dict:
    web.drawer("Timeline")
    heading = web.script("const e=document.querySelector('#analysis-tool-label'),r=e.getBoundingClientRect(),t=document.querySelector('.analysis-tabs').getBoundingClientRect();return {tag:e.tagName,label:e.textContent.trim(),clickable:!!e.closest('button,a,summary,[role=button]'),aboveTabs:r.bottom<=t.top+1};")
    assert heading == {"tag": "H3", "label": "Analysis", "clickable": False, "aboveTabs": True}, heading
    wait_for(lambda: web.script("return !!document.querySelector('.timeline-summary');"), bool, "real timestamp histogram opens")
    web.named("Show all", ".analysis-drawer")
    wait_for(lambda: web.script("return Number(document.querySelector('.timeline-summary')?.dataset.outside??-1);"), lambda count: count == 0, "all sparse and dense events included")
    marker = wait_for(lambda: web.script("return document.querySelector('.timeline-flag-marker[data-flag=\"critical\"][data-count=\"1\"]');"), bool, "single flagged event has a visible presence marker")
    geometry = web.script("const e=arguments[0],r=e.getBoundingClientRect(),s=getComputedStyle(e);return {width:r.width,height:r.height,opacity:s.opacity,display:s.display,bucket:e.dataset.bucketIndex};", marker)
    assert geometry["width"] >= 4 and geometry["height"] >= 4 and geometry["opacity"] == "1" and geometry["display"] != "none", geometry
    ticks = web.script("return [...document.querySelectorAll('.timeline-axis-tick')].map(e=>({text:e.textContent,title:e.querySelector('title')?.textContent??''}));")
    assert len(ticks) >= 3 and all(tick["text"].strip() and tick["title"] for tick in ticks), ticks
    window = web.script("return document.querySelector('.timeline-window-range').textContent;")
    assert "2026" in window and "123456789" in window, window
    index = geometry["bucket"]
    wait_for(lambda: web.script("return document.querySelector('.timeline-bucket-detail')?.dataset.bucketIndex;"), lambda value: value == index, "the flagged bucket is inspectable by default")
    bucket = web.script("const e=document.querySelector('.timeline-bucket[data-bucket-index=\"'+arguments[0]+'\"]');return {total:Number(e.dataset.count),critical:Number(e.dataset.critical),title:e.querySelector('title').textContent};", index)
    details = web.script("return {start:document.querySelector('.timeline-bucket-start').textContent,end:document.querySelector('.timeline-bucket-end').textContent,counts:[...document.querySelectorAll('.timeline-detail-count')].map(e=>({flag:e.dataset.flag,count:Number(e.dataset.count)}))};")
    assert bucket["total"] == bucket["critical"] == 1 and sum(item["count"] for item in details["counts"]) == 1, (bucket, details)
    assert details["start"] in bucket["title"] and details["end"] in bucket["title"], (bucket, details)
    reads_before = sum(entry["name"] == "histogram" for entry in web.trace())
    web.script("document.querySelector('.timeline-svg').focus();")
    web.request("POST", web.route("actions"), {"actions": [{"type": "key", "id": "timeline-detail-inspect", "actions": [
        {"type": "keyDown", "value": "\ue012"}, {"type": "keyUp", "value": "\ue012"}]}]})
    wait_for(lambda: web.script("return document.querySelector('.timeline-bucket-detail').dataset.bucketIndex;"),
        lambda value: value == str(max(0, int(index) - 1)), "keyboard inspection selects a neighboring interval")
    point = web.script("const r=arguments[0].getBoundingClientRect();return {x:Math.round((r.left+r.right)/2),y:Math.round((r.top+r.bottom)/2)};", marker)
    web.request("POST", web.route("actions"), {"actions": [{"type": "pointer", "id": "timeline-detail-pointer", "parameters": {"pointerType": "mouse"}, "actions": [
        {"type": "pointerMove", **point, "duration": 0, "origin": "viewport"},
        {"type": "pointerMove", "x": 40, "y": 20, "duration": 0, "origin": "viewport"}]}]})
    _settle(web)
    assert web.script("return document.querySelector('.timeline-bucket-detail').dataset.bucketIndex;") == index
    assert sum(entry["name"] == "histogram" for entry in web.trace()) == reads_before, "Inspecting intervals must not aggregate again"
    web.screenshot(evidence / "followup-single-flag-timeline.png")
    return {"separate_noninteractive_category": heading, "single_flag_marker": geometry,
        "axis_ticks": ticks, "exact_window_range": window, "bucket": bucket, "details": details,
        "keyboard_and_hover_inspection_without_requery": True}


def verify_detail_timeline(web, data, root: Path, evidence: Path, report: dict) -> None:
    message = 'A long source value with "quotes", comma, colon: and newline\n' + "literal\\path and original text " * 24
    source = root / f"detail-timeline-{uuid.uuid4().hex[:8]}.jsonl"
    records = [{"id": f"row-{i:03d}", "timestamp": "2026-11-12T12:00:00.123456789Z" if i < 200 else
        "2026-11-12T12:00:10.987654321Z" if i == 200 else "2026-11-12T12:00:20.123456789Z",
        "message": message if i == 0 else f"ordinary-{i}", "huge": 18446744073709551615 if i == 0 else i,
        "nullable": None if i == 0 else "present", "empty": "" if i == 0 else "present"} for i in range(202)]
    source.write_text("".join(json.dumps(row) + "\n" for row in records), encoding="utf-8")
    case = web.command("create_project", {"path": str(source)})
    project_id = case["meta"]["id"]
    web.command("update_flag", {"projectId": project_id, "row_index": 200, "flag": "critical"})
    try:
        open_project(web, case["meta"]["name"])
        web.results(len(records))
        detail = web.command("load_project", {"projectId": project_id}, request=True)
        columns = {column["label"]: column["id"] for column in detail["columns_meta"]}
        report["followup_detail"] = _details(web, project_id, columns, message, len(records), evidence)
        report["followup_timeline"] = _timeline(web, project_id, evidence)
    finally:
        if web.script("return !!document.querySelector('.row-detail-dialog');"):
            web.named("Close", ".row-detail-dialog")
        web.close_drawer()
        web.tab(data["id"])
        web.results(data["total"])
        web.named(f"Close {source.name}")
        web.command("delete_project", {"projectId": project_id}, request=True)
        open_project(web, data["name"])
        web.results(data["total"])
