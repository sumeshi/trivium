"""Focused real-WebView regression for investigation UI feedback.

No app state is mocked. The loading check delays delivery of one real native
query response; form-helper values use DOM input events, followed by a trusted
click on Insert UTC. Flags, memos and existing colour rules are preserved.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from row_navigation import wait_for


def _settle(web) -> None:
    web.async_script("const done=arguments[arguments.length-1];requestAnimationFrame(()=>requestAnimationFrame(()=>done(true)));")


def _horizontal_to(web, target: int) -> None:
    wait_for(lambda: web.script("return !document.querySelector('.drawer-backdrop');"), bool, "horizontal controls unobstructed")
    _settle(web)
    # A trusted header click settles focus after a case remount. If WebKit
    # swallows that first wheel, retry only the measured remaining distance.
    web.tap(web.element('.data-header .sticky-index'))
    for attempt in range(3):
        current = web.state()["scrollLeft"]
        if current == target:
            assert web.script("return document.querySelector('.table-header-scroll').scrollLeft;") == target
            return
        point = web.script("const r=document.querySelector('.table-header-scroll').getBoundingClientRect();return {x:Math.round(Math.min(r.left+600,innerWidth-20)),y:Math.round(r.top+r.height/2)};")
        web.request("POST", web.route("actions"), {"actions": [{"type": "wheel", "id": "feedback-wheel", "actions": [
            {"type": "scroll", **point, "deltaX": target-current, "deltaY": 0, "duration": 0}]}]})
        try:
            wait_for(lambda: web.script("return {body:document.querySelector('.virtual-viewport').scrollLeft,header:document.querySelector('.table-header-scroll').scrollLeft};"),
                lambda value: value["body"] == target and value["header"] == target, "trusted horizontal wheel moves body and header", timeout=2)
            return
        except AssertionError:
            if attempt == 2:
                raise
            _settle(web)


def _clear(web, total: int) -> None:
    web.close_drawer()
    if web.script("return !document.querySelector('[aria-label=\"Clear all filters\"]').disabled;"):
        web.named("Clear all filters")
    web.applied(search="", searchColumns=[], flagFilter="all", bookmarkedOnly=False, condition=None)
    web.results(total)


def _header_and_search(web, data, evidence, report) -> None:
    header = web.script("""
      const name=document.querySelector('.project-title'),sha=document.querySelector('.sha-button');
      const a=name.getBoundingClientRect(),b=sha.getBoundingClientRect();
      return {filename:name.textContent.trim(),hash:sha.title,headerHeight:document.querySelector('.app-header').getBoundingClientRect().height,
        sameLine:Math.abs((a.top+a.bottom)/2-(b.top+b.bottom)/2)<3,hashAfterName:b.left>=a.right-1};
    """)
    assert header["filename"] == data["filename"] and data["sha"] in header["hash"], header
    assert header["sameLine"] and header["hashAfterName"] and header["headerHeight"] <= 40, header
    assert web.script("return [...document.querySelectorAll('.tool-groups > button')].map(e=>e.textContent.trim());") == ["Filters", "Analysis", "Annotations"]
    export = web.script("const e=document.querySelector('[aria-label=\"Export rows\"]');return e&&{text:e.textContent.trim(),title:e.title};")
    assert export and export["text"] == "" and export["title"], export
    web.named("Workspace options")
    assert "0.2.0" in web.script("return document.querySelector('[aria-label=\"Application version\"]').textContent;")
    web.named("Workspace options")
    web.named("Open project menu")
    recent = web.script("return [...document.querySelectorAll('.project-card')].find(e=>e.querySelector('.project-name')?.textContent.trim()===arguments[0])?.querySelector('.project-hash')?.title;", data["name"])
    assert recent and data["sha"] in recent, recent
    web.named("Close workspace menu", ".workspace-drawer")
    wait_for(lambda: web.script("return !document.querySelector('.drawer-backdrop');"), bool, "project drawer backdrop removed")
    web.named("Search syntax help")
    help_text = web.script("return document.querySelector('#search-syntax-help').textContent;")
    assert all(token in help_text for token in ("error timeout", "error | warning", "-noise", "column:value", "regular expressions")), help_text
    web.named("Done", "#search-syntax-help")
    before = web.state()["headers"]
    web.named("Choose search columns")
    checkbox = web.script("return [...document.querySelectorAll('#search-column-menu label')].find(e=>e.textContent.trim()==='cohort')?.querySelector('input');")
    web.click(checkbox)
    web.named("Done", "#search-column-menu")
    web.write('input[aria-label="Search rows"]', "keep")
    web.applied(search="keep", searchColumns=[data["columns"]["cohort"]])
    web.results(data["total"] - data["total"] // 5)
    assert web.state()["headers"] == before, "Search scope must not change visible columns"
    status = web.script("return document.querySelector('[aria-label=\"Applied filters\"]').textContent;")
    assert "Filtered" in status and "keep" in status and "cohort" in status, status
    web.screenshot(evidence / "feedback-applied-search.png")
    _clear(web, data["total"])
    assert web.script("return document.querySelector('[aria-label=\"Applied filters\"]').textContent.includes('No filters applied');")
    report["header_and_search"] = {**header, "compact_categories": True, "export_icon": True,
        "version_visible": True, "recent_hash": True, "syntax_and_independent_scope": True, "applied_status_and_clear": True}


def _column_and_dates(web, data, report) -> None:
    web.named("Actions for column cohort")
    web.named("Search this column", ".column-context-menu")
    # An unused search scope is saved by the picker without requerying rows.
    web.applied(search="", searchColumns=[])
    assert "cohort" in web.script("return document.querySelector('[aria-label=\"Choose search columns\"]').textContent;")
    assert web.script("return document.activeElement?.getAttribute('aria-label');") == "Search rows"
    _clear(web, data["total"])
    web.named("Actions for column source_id")
    web.named("Add rule for this column…", ".column-context-menu")
    wait_for(lambda: web.script("return document.querySelector('#tool-tab-conditions')?.getAttribute('aria-selected');"), lambda x: x == "true", "column action opens Rules")
    web.results(data["total"])
    assert web.script("return !!document.activeElement?.hasAttribute('data-condition-id');"), "The new rule's value is focused"
    web.choose('select[aria-label="Column"]', data["columns"]["timestamp"])
    web.choose('select[aria-label="Operator"]', "ge")
    web.write('input[aria-label="Value"]', "not-a-timestamp")
    wait_for(lambda: web.script("return document.querySelector('input[aria-label=\"Value\"]').getAttribute('aria-invalid');"), lambda x: x == "true", "invalid datetime is visibly marked")
    instant = "2026-11-11T12:00:00.123456789Z"
    offset = "2026-11-11T21:00:00.123456789+09:00"
    accepted = web.command("validate_condition_values", {"kind": "datetime", "op": "ge", "values": [instant, offset]})
    assert accepted["valid"] and accepted["values"][0]["parsedNanos"] == accepted["values"][1]["parsedNanos"], accepted
    invalid = web.command("validate_condition_values", {"kind": "datetime", "op": "ge", "values": ["2026-13-42"]})
    assert not invalid["valid"] and invalid["values"][0]["error"], invalid
    web.write('input[aria-label="Value"]', offset)
    wait_for(lambda: web.script("return document.querySelector('.value-check').textContent;"), lambda s: "Timestamp format accepted" in s, "real engine accepts nanoseconds and offset")
    web.named("Choose UTC date and time")
    assert web.script("return document.querySelector('input[aria-label=\"Value\"]').value;") == offset
    web.script("for(const [label,value] of [['UTC date','2026-11-12'],['UTC time','13:14:15']]){const e=document.querySelector('input[aria-label=\"'+label+'\"]');e.value=value;e.dispatchEvent(new Event('input',{bubbles:true}));}")
    web.named("Insert UTC")
    wait_for(lambda: web.script("return document.querySelector('input[aria-label=\"Value\"]').value;"), lambda s: s == "2026-11-12T13:14:15Z", "explicit picker insertion uses UTC")
    # The footer must remain usable while its associated Rules panel is open,
    # including when an earlier invalid-input notification is still visible.
    web.named("Clear all filters")
    web.applied(search="", searchColumns=[], flagFilter="all", bookmarkedOnly=False, condition=None)
    web.results(data["total"])
    assert web.script("return !!document.querySelector('#analysis-drawer');"), "Clearing filters must not require closing Rules"
    report["column_and_dates"] = {"column_search_focus": True, "blank_rule_keeps_results": True,
        "invalid_marked": True, "nanoseconds_preserved": True, "offset_same_instant": True,
        "picker_open_preserves_text": True, "explicit_utc_insert": True, "clear_with_rules_open": True,
        "picker_form_fill": "DOM input events; trusted Insert UTC click"}
    _clear(web, data["total"])


# Inspect the actual paint layer and real hit-test target at every pinned cell.
_STICKY = """
const canvas=document.createElement('canvas');canvas.width=canvas.height=1;
const context=canvas.getContext('2d');
const opaque=colour=>{context.clearRect(0,0,1,1);context.fillStyle=colour;context.fillRect(0,0,1,1);return context.getImageData(0,0,1,1).data[3]===255;};
const viewport=document.querySelector('.virtual-viewport').getBoundingClientRect();
const rows=[...document.querySelectorAll(arguments[0])].filter(row=>{const r=row.getBoundingClientRect();return r.top>=viewport.top&&r.bottom<=viewport.bottom;}).slice(0,3);
rows.unshift(document.querySelector('.data-header'));
const result=[];let overlaps=0;
for(const row of rows){const moving=[...row.children].filter(c=>!c.classList.contains('sticky'));
 for(const cell of row.querySelectorAll(':scope > .sticky')){const r=cell.getBoundingClientRect(),s=getComputedStyle(cell),rs=getComputedStyle(row),hit=document.elementFromPoint(Math.round((r.left+r.right)/2),Math.round((r.top+r.bottom)/2));
  overlaps+=moving.filter(m=>{const a=m.getBoundingClientRect();return a.left<r.right&&a.right>r.left;}).length;
  result.push({classes:cell.className,background:s.backgroundColor,opaque:opaque(s.backgroundColor),opacity:s.opacity,rowOpacity:rs.opacity,topmost:!!hit&&cell.contains(hit)});
 }}
const custom=rows.find(r=>r.style.getPropertyValue('--row-color'));
const plain=rows.find(r=>r.classList.contains('data-row')&&!r.style.getPropertyValue('--row-color')&&!r.classList.contains('alt-row'));
return {cells:result,overlaps,customColourApplied:!!custom&&!!plain&&getComputedStyle(custom).backgroundColor!==getComputedStyle(plain).backgroundColor,scrollLeft:document.querySelector('.virtual-viewport').scrollLeft,theme:document.documentElement.dataset.theme};
"""


def _check_sticky(web, selector: str, label: str) -> dict[str, Any]:
    state = web.script(_STICKY, selector)
    assert len(state["cells"]) >= 10 and state["overlaps"] > 0 and state["scrollLeft"] >= 699, (label, state)
    assert all(c["opaque"] and c["opacity"] == "1" and c["rowOpacity"] == "1" and c["topmost"] for c in state["cells"]), (label, state)
    return state


def _theme(web, name: str) -> None:
    web.named("Workspace options")
    web.choose('select[aria-label="Theme"]', name)
    web.named("Workspace options")
    wait_for(lambda: web.script("return document.documentElement.dataset.theme;"), lambda value: value == name, f"{name} is active")


def _sticky_feedback(web, data, evidence, report) -> None:
    original = web.command("color_rules", {"projectId": data["id"]}, request=True)["rules"]
    samples = []
    try:
        web.command("save_color_rules", {"projectId": data["id"], "rules": [{"name": "native pinned-row proof", "color": "#26364f",
            "condition": {"column": data["columns"]["source_id"], "op": "eq", "values": ["r-0000001"], "negated": False}}]})
        web.named("Workspace options"); web.named("Reload saved project")
        web.results(data["total"])
        wait_for(lambda: web.script("return !!document.querySelector('.data-row[style*=\"--row-color\"]');"), bool, "real native colour rule applied")
        _settle(web)
        # The header wheel is a trusted action and must move the real body.
        _horizontal_to(web, 700)
        for name in ("trivium", "light", "mauve"):
            _theme(web, name); _settle(web)
            sample = _check_sticky(web, ".data-row:not(.loading)", name)
            assert sample["customColourApplied"], (name, sample)
            first = web.node('.data-row .stickyData')
            point = web.script("const r=arguments[0].getBoundingClientRect();return {x:Math.round((r.left+r.right)/2),y:Math.round((r.top+r.bottom)/2)};", first)
            web.request("POST", web.route("actions"), {"actions": [{"type": "pointer", "id": "feedback-hover", "parameters": {"pointerType": "mouse"}, "actions": [{"type": "pointerMove", **point, "duration": 0, "origin": "viewport"}]}]})
            _settle(web); sample["hover"] = _check_sticky(web, ".data-row:not(.loading)", name + " hover")
            samples.append(sample)
            web.screenshot(evidence / f"feedback-sticky-{name}.png")
        _theme(web, "trivium")
        web.script("window.__workspaceHold={name:'query_project_rows',projectId:arguments[0],search:'keep',ready:false,release:null};", data["id"])
        web.write('input[aria-label="Search rows"]', "keep")
        wait_for(lambda: web.script("return window.__workspaceHold.ready;"), bool, "real native query response held for loading-row check")
        wait_for(lambda: web.script("return document.querySelectorAll('.data-row.loading').length;"), lambda n: n >= 2, "loading rows displayed")
        _horizontal_to(web, 700)
        loading = _check_sticky(web, ".data-row.loading", "loading")
        web.screenshot(evidence / "feedback-sticky-loading.png")
        web.script("window.__workspaceHold.release();delete window.__workspaceHold;")
        web.applied(search="keep"); web.results(data["total"] - data["total"] // 5)
        report["sticky_paint"] = {"samples": samples, "loading": loading, "actual_horizontal_scroll": 700,
            "normal_alternate_hover_custom_rows": True, "opaque_topmost_pinned_cells": True}
    finally:
        web.script("window.__workspaceHold?.release?.();delete window.__workspaceHold;")
        _clear(web, data["total"])
        web.command("save_color_rules", {"projectId": data["id"], "rules": original})
        web.named("Workspace options"); web.named("Reload saved project")
        web.results(data["total"])
        _theme(web, "trivium")
        _horizontal_to(web, 0)


def verify_feedback(web, data: dict[str, Any], root: Path, evidence: Path, report: dict[str, Any]) -> None:
    checks: dict[str, Any] = {}
    _header_and_search(web, data, evidence, checks)
    _column_and_dates(web, data, checks)
    _sticky_feedback(web, data, evidence, checks)
    report["investigation_feedback"] = checks
