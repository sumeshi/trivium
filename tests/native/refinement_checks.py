"""Native regressions for scope-only edits, row styling and form clarity."""
from __future__ import annotations

import time
from pathlib import Path

from feedback_checks import _check_sticky, _clear, _horizontal_to, _settle, _theme
from refinement_imports import verify_import_identity
from refinement_grouping import verify_grouping
from row_navigation import wait_for

READS = {"query_project_rows", "locate_project_row", "column_stats", "histogram", "time_analysis", "group_tree", "get_cell", "surrounding_rows"}


def _scope(web, label: str) -> None:
    web.named("Choose search columns")
    checkbox = web.script("return [...document.querySelectorAll('#search-column-menu label')].find(e=>e.textContent.trim()===arguments[0])?.querySelector('input');", label)
    assert checkbox, label
    web.click(checkbox)
    web.named("Done", "#search-column-menu")


def _scope_without_search(web, data, report) -> None:
    _clear(web, data["total"])
    web.write('.row-jump input[type="number"]', "242")
    web.button("Go")
    wait_for(web.state, lambda state: any(row["id"] == 242 and row["top"] >= state["top"] for row in state["rows"]), "source row242 visible")
    for row in (242, 243):
        web.click(web.element(f'input[aria-label="Select row {row}"]'))
    _settle(web)
    before = web.state()
    assert sorted(before["selectedIds"]) == [242, 243], before
    start = len(web.trace())
    _scope(web, "cohort")
    web.write('input[aria-label="Search rows"]', "   ")
    _scope(web, "cohort")
    _scope(web, "source_id")
    time.sleep(.4)  # Wait beyond the real search debounce; no reads should start.
    _settle(web)
    after = web.state()
    reads = [entry for entry in web.trace()[start:] if entry["name"] in READS]
    assert not reads, reads
    assert abs(after["scrollTop"] - before["scrollTop"]) <= 1 and after["scrollLeft"] == before["scrollLeft"], (before, after)
    assert after["selectedIds"] == before["selectedIds"] and after["selectionTotal"] == 2, (before, after)
    web.write('input[aria-label="Search rows"]', "keep")
    web.applied(search="keep", searchColumns=[data["columns"]["source_id"]])
    web.results(0)
    _scope(web, "cohort")
    _scope(web, "source_id")
    web.applied(search="keep", searchColumns=[data["columns"]["cohort"]])
    web.results(data["total"] - data["total"] // 5)
    report["scope_only_edits"] = {"empty_and_whitespace": True, "native_read_count": 0,
        "selected_original_ids": [242, 243], "vertical_and_horizontal_position_preserved": True,
        "real_text_search_respects_scope": True}
    _clear(web, data["total"])


_CHECKBOX = """
const e=document.querySelector('[aria-label="Select visible rows"]'),s=getComputedStyle(e),m=getComputedStyle(e,'::before');
return {checked:e.checked,mixed:e.indeterminate,appearance:s.appearance||s.webkitAppearance,background:s.backgroundColor,
  transform:m.transform,markerHeight:parseFloat(m.height),focusVisible:e.matches(':focus-visible'),outline:s.outlineStyle,outlineWidth:parseFloat(s.outlineWidth)};
"""


def _checkboxes(web, report) -> None:
    header = web.element('input[aria-label="Select visible rows"]')
    initial = web.script(_CHECKBOX)
    assert not initial["checked"] and not initial["mixed"] and initial["appearance"] == "none", initial
    web.click(header)
    checked = wait_for(lambda: web.script(_CHECKBOX), lambda value: value["checked"] and not value["mixed"], "themed checked header checkbox")
    assert checked["transform"] != initial["transform"] and checked["markerHeight"] >= 8, (initial, checked)
    first = web.script("return document.querySelector('.data-row input[aria-label^=\"Select row \"]');")
    web.click(first)
    mixed = wait_for(lambda: web.script(_CHECKBOX), lambda value: value["mixed"], "themed mixed header checkbox")
    assert mixed["markerHeight"] < checked["markerHeight"] and mixed["transform"] != initial["transform"], mixed
    web.script("arguments[0].focus();", header)
    web.request("POST", web.route("actions"), {"actions": [{"type": "key", "id": "refinement-checkbox-focus", "actions": [
        {"type": "keyDown", "value": "\ue004"}, {"type": "keyUp", "value": "\ue004"},
        {"type": "keyDown", "value": "\ue008"}, {"type": "keyDown", "value": "\ue004"},
        {"type": "keyUp", "value": "\ue004"}, {"type": "keyUp", "value": "\ue008"}]}]})
    focused = wait_for(lambda: web.script(_CHECKBOX), lambda value: value["focusVisible"], "trusted keyboard checkbox focus")
    assert focused["outline"] != "none" and focused["outlineWidth"] >= 2, focused
    for _ in range(2):
        state = web.script(_CHECKBOX)
        if not state["checked"] and not state["mixed"]:
            break
        web.click(header)
    assert web.state()["selectionTotal"] == 0
    report["checkboxes"] = {"unchecked": initial, "checked": checked, "mixed": mixed, "keyboard_focus": focused}


_ROW_STYLE = """
const canvas=document.createElement('canvas');canvas.width=canvas.height=1;const c=canvas.getContext('2d');
const rgba=v=>{c.clearRect(0,0,1,1);c.fillStyle=v;c.fillRect(0,0,1,1);return [...c.getImageData(0,0,1,1).data];};
const row=id=>[...document.querySelectorAll('.data-row')].find(e=>e.querySelector('.cell.index')?.textContent.trim()===String(id));
const distance=(a,b)=>Math.hypot(...a.slice(0,3).map((n,i)=>n-b[i]));
const sample=id=>{const e=row(id),s=getComputedStyle(e),bg=rgba(s.backgroundColor),gold=rgba(s.getPropertyValue('--bookmark'));
 const base=rgba(s.getPropertyValue('--row-color')||s.getPropertyValue('--row-base')||s.getPropertyValue('--surface'));
 return {id,bookmarked:e.classList.contains('bookmarked'),custom:!!e.style.getPropertyValue('--row-color'),bg,gold,
  closerToGold:distance(bg,gold)<distance(base,gold),tintFraction:distance(bg,base)/distance(gold,base),marker:getComputedStyle(e.querySelector('.select-cell')).boxShadow,
  pinned:[...e.querySelectorAll(':scope>.sticky')].map(x=>({alpha:rgba(getComputedStyle(x).backgroundColor)[3],opacity:getComputedStyle(x).opacity}))};};
const lum=rgb=>{const a=rgb.slice(0,3).map(n=>{n/=255;return n<=.04045?n/12.92:((n+.055)/1.055)**2.4;});return a[0]*.2126+a[1]*.7152+a[2]*.0722;};
const contrast=(a,b)=>{const x=lum(a),y=lum(b);return (Math.max(x,y)+.05)/(Math.min(x,y)+.05);};
const empty=row(3).querySelector('.memo-button'),filled=row(10).querySelector('.memo-button');
return {theme:document.documentElement.dataset.theme,rows:[sample(1),sample(2)],memo:{
 emptyText:empty.textContent.trim(),filledText:filled.textContent.trim(),emptyOpacity:getComputedStyle(empty).opacity,
 emptyContrast:contrast(rgba(getComputedStyle(empty).color),rgba(getComputedStyle(row(3)).backgroundColor)),
 filledContrast:contrast(rgba(getComputedStyle(filled).color),rgba(getComputedStyle(row(10)).backgroundColor))}};
"""


def _bookmarked_rows(web, data, evidence: Path, report) -> None:
    original_rules = web.command("color_rules", {"projectId": data["id"]}, request=True)["rules"]
    old = web.command("query_project_rows", {"projectId": data["id"], "offset": 0, "limit": 2})["rows"]
    samples = []
    try:
        for index in (0, 1):
            web.command("update_flag", {"projectId": data["id"], "row_index": index, "bookmarked": True})
        web.command("save_color_rules", {"projectId": data["id"], "rules": [{"name": "native bookmarked rule", "color": "#26364f",
            "condition": {"column": data["columns"]["source_id"], "op": "eq", "values": ["r-0000001"], "negated": False}}]})
        web.named("Workspace options"); web.named("Reload saved project")
        web.results(data["total"])
        web.button("First")
        wait_for(lambda: web.script("return document.querySelectorAll('.data-row.bookmarked').length;"), lambda value: value == 2, "native bookmark edits colour both rows")
        # Scroll on the table body, where an investigator's row gesture lands.
        # Header-wheel forwarding is independently covered by verify_feedback.
        point = web.script("const e=document.querySelector('.virtual-viewport'),r=e.getBoundingClientRect();return {x:Math.round(r.left+Math.min(650,r.width-30)),y:Math.round(r.top+80)};")
        web.request("POST", web.route("actions"), {"actions": [{"type": "wheel", "id": "refinement-body-wheel", "actions": [
            {"type": "scroll", **point, "deltaX": 700, "deltaY": 0, "duration": 0}]}]})
        wait_for(lambda: web.script("return document.querySelector('.virtual-viewport').scrollLeft;"), lambda value: value == 700, "trusted body wheel reveals bookmark fixed-cell overlap")
        for name in ("light", "dark", "yukari"):
            _theme(web, name)
            web.script("document.querySelector('.table-meta').focus?.();")
            _settle(web)
            sample = web.script(_ROW_STYLE)
            assert sample["rows"][0]["custom"] and not sample["rows"][1]["custom"], sample
            for row in sample["rows"]:
                assert row["bookmarked"] and row["bg"][3] == 255 and row["closerToGold"] and row["marker"] != "none", row
                assert row["gold"][0] > row["gold"][2] and row["gold"][1] > row["gold"][2], row
                assert 0 < row["tintFraction"] < .12, "Bookmark tint stays subtle over the original row colour"
                assert all(cell["alpha"] == 255 and cell["opacity"] == "1" for cell in row["pinned"]), row
            assert sample["memo"]["emptyText"] == "Add memo" and sample["memo"]["filledText"] == "Persist native audit", sample
            assert sample["memo"]["emptyOpacity"] == "1" and sample["memo"]["emptyContrast"] < sample["memo"]["filledContrast"], sample
            sample["sticky"] = _check_sticky(web, ".data-row:not(.loading)", name + " bookmarked")
            samples.append(sample)
            web.screenshot(evidence / f"refinement-bookmarks-{name}.png")
    finally:
        for row in old:
            web.command("update_flag", {"projectId": data["id"], "row_index": row["row_index"], "bookmarked": row["bookmarked"]})
        web.command("save_color_rules", {"projectId": data["id"], "rules": original_rules})
        _theme(web, "trivium")
        web.named("Workspace options"); web.named("Reload saved project")
        web.results(data["total"])
        _horizontal_to(web, 0)
    report["bookmark_and_memo_styling"] = samples


def verify_refinements(web, data, root: Path, evidence: Path, report: dict) -> None:
    checks = {}
    verify_import_identity(web, data, root, checks)
    _scope_without_search(web, data, checks)
    _checkboxes(web, checks)
    _bookmarked_rows(web, data, evidence, checks)
    verify_grouping(web, data, root, evidence, checks)
    report["refinements"] = checks
