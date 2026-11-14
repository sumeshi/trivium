"""Table-only text sizing leaves navigation, evidence and other UI geometry intact."""
from __future__ import annotations

import csv
import json
import math
import os
from pathlib import Path
import uuid

from column_pin_checks import _prefix
from feedback_checks import _horizontal_to, _settle
from row_navigation import open_project, wait_for


BASE = {"body": 10.5, "header": 9.5, "index": 10, "note": 11, "flag": 10, "tag": 10}


def _fit_failure(percent: int, index: int, control: dict) -> None:
    """Public diagnostics contain only fixed labels and bounded numeric geometry."""
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return
    safe = {"control": "tag" if control["tag"] else "flag", "index": index}
    if percent in {85, 100, 115, 130, 145}:
        safe["percent"] = percent
    for key in ("width", "height", "contentWidth", "contentHeight", "advanceWidth", "inkWidth", "inkHeight", "fontSize"):
        value = control.get(key)
        if isinstance(value, (int, float)) and math.isfinite(value) and abs(value) <= 10_000:
            safe[key] = round(value, 3)
    print(f"::error title=Native table text fit::{json.dumps(safe, separators=(',', ':'))}", flush=True)


def _size(web, action: str | None, expected: int) -> None:
    if not web.script("return !!document.querySelector('.workspace-menu')?.open;"):
        web.named("Workspace options")
    wait_for(lambda: web.script("return !!document.querySelector('.workspace-menu')?.open;"), bool, "workspace settings are open")
    _settle(web)
    if action:
        web.tap(web.node(f'[aria-label="{action} table text size"]'))
    last_controls = None

    def read_controls():
        nonlocal last_controls
        last_controls = web.script("""
          const g=document.querySelector('[aria-label="Table text size"]');
          return {text:g.querySelector('[aria-label="Reset table text size"]').textContent.trim(),
            disabled:[...g.querySelectorAll('button')].map(e=>e.disabled)};
        """)
        return last_controls

    desired = {"text": f"{expected}%", "disabled": [expected == 85, expected == 100, expected == 145]}
    try:
        wait_for(read_controls, lambda controls: controls == desired, "table text controls update", timeout=8)
    except AssertionError as error:
        if os.environ.get("GITHUB_ACTIONS") == "true" and str(error).startswith("Timed out: table text controls update;"):
            sizes = {85, 100, 115, 130, 145}
            observed = next((size for size in sizes if (last_controls or {}).get("text") == f"{size}%"), None)
            safe = {"action": action if action in {"Decrease", "Increase", "Reset"} else "read",
                    "expected": expected if expected in sizes else None, "observed": observed}
            disabled = (last_controls or {}).get("disabled")
            if isinstance(disabled, list) and len(disabled) == 3 and all(type(value) is bool for value in disabled):
                safe["disabled"] = disabled
            print(f"::error title=Native table text state::{json.dumps(safe, separators=(',', ':'))}", flush=True)
        raise
    web.named("Workspace options")
    _settle(web)
    if action:
        assert web.script("return localStorage.getItem('trivium:table-text-size');") == str(expected)


def _sample(web, percent: int) -> dict:
    sample = web.script("""
      const row=document.querySelector('.data-row:not(.loading)'),font=e=>parseFloat(getComputedStyle(e).fontSize),
        box=e=>{const r=e.getBoundingClientRect();return [r.left,r.top,r.width,r.height];},
        index=row.querySelector('.index'),s=getComputedStyle(index),canvas=document.createElement('canvas');
      const context=canvas.getContext('2d'),canvasFont=style=>`${style.fontStyle} ${style.fontWeight} ${style.fontSize} ${style.fontFamily}`,
        contentBox=e=>{const r=e.getBoundingClientRect(),c=getComputedStyle(e);return {
          width:r.width-parseFloat(c.borderLeftWidth)-parseFloat(c.borderRightWidth)-parseFloat(c.paddingLeft)-parseFloat(c.paddingRight),
          height:r.height-parseFloat(c.borderTopWidth)-parseFloat(c.borderBottomWidth)-parseFloat(c.paddingTop)-parseFloat(c.paddingBottom)};};
      context.font=canvasFont(s);const sevenDigitWidth=context.measureText('2000000').width;
      return {fonts:{body:font(row.querySelector('.cell-text')),header:font(document.querySelector('.data-header .header-label')),
        index:font(index),note:font(row.querySelector('.memo-button')),flag:font(row.querySelector('.flag-safe')),
        tag:font(row.querySelector('.row-tags-button'))},
        rowHeights:[...document.querySelectorAll('.data-row')].map(e=>e.getBoundingClientRect().height),
        headerHeight:document.querySelector('.data-header').getBoundingClientRect().height,
        grid:document.querySelector('.data-header').style.gridTemplateColumns,
        outside:[...document.querySelectorAll('.brand-name,.filter-row,.filter-row input,.aux-trigger,.table-meta')].map(e=>({font:font(e),box:box(e)})),
        controls:[...row.querySelectorAll('.flag-buttons button,.row-tags-button')].map(e=>{
          const r=e.getBoundingClientRect(),tag=e.classList.contains('row-tags-button'),label=tag?e.querySelector('span'):e,
            style=getComputedStyle(label),content=contentBox(e),labelWidth=contentBox(label).width;
          // clientWidth rounds fractional glyph widths; compare the same float CSS-pixel units.
          context.font=canvasFont(style);const m=context.measureText(label.textContent.trim()),
            inkWidth=m.actualBoundingBoxLeft+m.actualBoundingBoxRight,inkHeight=m.actualBoundingBoxAscent+m.actualBoundingBoxDescent;
          return {height:r.height,width:r.width,tag,fontSize:parseFloat(style.fontSize),contentWidth:labelWidth,
            contentHeight:content.height,advanceWidth:m.width,inkWidth,inkHeight,
            fits:Math.max(m.width,inkWidth)<=labelWidth+.1&&inkHeight<=content.height};}),
        sevenDigitWidth,
        indexContentWidth:contentBox(index).width};
    """)
    for kind, base in BASE.items():
        assert abs(sample["fonts"][kind] - base * percent / 100) <= .03, (percent, kind, sample)
    assert all(height == 32 for height in sample["rowHeights"]) and sample["headerHeight"] == 38, sample
    assert sample["sevenDigitWidth"] <= sample["indexContentWidth"] + .1, sample
    failed = [(index, control) for index, control in enumerate(sample["controls"])
              if not control["fits"] or control["height"] != (20 if control["tag"] else 21)]
    if failed:
        _fit_failure(percent, *failed[0])
    assert not failed, sample
    return sample


def _change(web, total: int, action: str, percent: int, baseline: dict) -> dict:
    before, query, start = web.state(), web.query(), len(web.trace())
    _size(web, action, percent)
    after, sample = web.results(total), _sample(web, percent)
    assert sample["grid"] == baseline["grid"] and sample["outside"] == baseline["outside"], sample
    assert web.query() == query and after["selectionTotal"] == before["selectionTotal"]
    assert [row["id"] for row in after["rows"]] == [row["id"] for row in before["rows"]]
    assert abs(after["scrollTop"] - before["scrollTop"]) <= 1 and after["scrollLeft"] == before["scrollLeft"]
    assert not [entry for entry in web.trace()[start:] if entry["name"] in {"query_project_rows", "locate_project_row"}], "Text size must not reload or locate data"
    return {"percent": percent, "scroll_top": after["scrollTop"], "scroll_left": after["scrollLeft"], "fonts": sample["fonts"]}


def _dialog(web) -> dict:
    web.tap(web.node('.data-row[data-row-index="1"] .memo-button'))
    wait_for(lambda: web.script("return !!document.querySelector('.memo-dialog');"), bool, "note editor opens at this text size")
    _settle(web)
    return web.script("const e=document.querySelector('.memo-dialog'),r=e.getBoundingClientRect();return {width:r.width,height:r.height,font:getComputedStyle(e).fontSize,inputFont:getComputedStyle(e.querySelector('textarea')).fontSize};")


def verify_table_text(app, data, root: Path, evidence: Path, report: dict) -> None:
    web = app.web
    source = root / f"table-text-{uuid.uuid4().hex[:8]}.csv"
    names = ["source_id", "cohort"] + [f"field_{i:02}" for i in range(14)]
    with source.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle); writer.writerow(names)
        writer.writerows([f"r-{i:07d}", "keep" if i % 2 == 0 else "other", *[f"sample-field-{j:02d}-record-{i:04d}-content" for j in range(14)]] for i in range(1, 601))
    original = source.read_bytes()
    case = web.command("create_project", {"path": str(source)})
    owner = case["meta"]["id"]
    patch = {"projectId": owner, "target": "all", "add_tags": ["text-size"]}
    token = web.command("bulk_preview", patch)["previewToken"]
    web.command("bulk_update", {**patch, "previewToken": token})
    other = root / f"table-text-other-{uuid.uuid4().hex[:8]}.csv"
    other.write_text("source_id\nsecond case\n", encoding="utf-8")
    second = web.command("create_project", {"path": str(other)})
    try:
        open_project(web, case["meta"]["name"]); web.results(600)
        web.write('input[aria-label="Search rows"]', "cohort:keep")
        web.applied(search="cohort:keep"); web.results(300)
        web.button("First"); web.result(300, 2, 2); _settle(web)
        _size(web, None, 100)
        baseline, dialog = _sample(web, 100), _dialog(web)
        web.named("Cancel", ".memo-dialog")
        web.drawer("Conditions")
        pane_script = "const e=document.querySelector('#analysis-drawer'),r=e.getBoundingClientRect();return {font:getComputedStyle(e.querySelector('h3')).fontSize,width:r.width,height:r.height};"
        pane = web.script(pane_script)
        web.close_drawer()
        web.click(web.node('input[aria-label="Select row 2"]'))
        checks = [_change(web, 300, "Decrease", 85, baseline), _change(web, 300, "Increase", 100, baseline)]
        web.go(300, 300, 2); _settle(web)
        checks.append(_change(web, 300, "Increase", 115, baseline))
        web.button("Last"); web.result(300, 600, 2); _settle(web)
        checks.append(_change(web, 300, "Increase", 130, baseline))
        web.button("First"); web.result(300, 2, 2); _settle(web)
        checks.append(_change(web, 300, "Increase", 145, baseline))
        web.screenshot(evidence / "table-text-largest.png")
        web.drawer("Conditions")
        assert web.script(pane_script) == pane
        _size(web, "Decrease", 130); _size(web, "Increase", 145)
        assert web.script(pane_script) == pane
        web.close_drawer()
        web.tap(web.node('.data-row[data-row-index="1"] .flag-safe'))
        wait_for(lambda: web.script("return document.querySelector('.data-row[data-row-index=\"1\"] .flag-safe').classList.contains('selected');"), bool, "large-text flag button is clickable")
        assert _dialog(web) == dialog, "Table text sizing must not alter note-dialog fonts or bounds"
        web.write('textarea[aria-label="Note"]', "Table text check")
        web.named("Save", ".memo-dialog")
        wait_for(lambda: web.script("return !document.querySelector('.memo-dialog');"), bool, "the note saves at the largest text size")
        saved = web.command("query_project_rows", {"projectId": owner, "offset": 1, "limit": 1})["rows"][0]
        assert saved["memo"] == "Table text check" and saved["flag"] == "safe" and saved["tags"] == ["text-size"], saved
        detail = web.command("load_project", {"projectId": owner}, request=True)
        first_column = detail["columns_meta"][0]["id"]
        _horizontal_to(web, 700); _prefix(web, [first_column], "large-text frozen column remains opaque and pinned")
        checks.append(_change(web, 300, "Decrease", 130, baseline))
        _prefix(web, [first_column], "changing text size keeps horizontal frozen geometry")
        _size(web, "Increase", 145)
        open_project(web, second["meta"]["name"]); web.results(1)
        assert abs(web.script("return parseFloat(getComputedStyle(document.querySelector('.data-row .cell-text')).fontSize);") - 10.5 * 1.45) <= .03
        web.tab(owner); web.results(300)
        assert web.state()["selectionTotal"] == 1
        def saved_sizes():
            try:
                return [json.loads(path.read_text(encoding="utf-8")) for path in root.rglob("table-text-size.json")]
            except (OSError, json.JSONDecodeError):
                return []
        wait_for(saved_sizes, lambda sizes: 145 in sizes, "145% is saved in the native preference file before restart")
        web.script("localStorage.setItem('trivium:table-text-size','130');")
        app.end_session(); web = app.start()
        _size(web, None, 145)
        assert web.script("return localStorage.getItem('trivium:table-text-size');") == "145"
        open_project(web, case["meta"]["name"]); web.results(300)
        _sample(web, 145)
        _size(web, "Reset", 100); _sample(web, 100)
        assert source.read_bytes() == original
        report["table_text"] = {"sizes_and_positions": checks, "default_fonts": baseline["fonts"], "row_height": 32,
            "header_height": 38, "seven_digit_row_number_fits": True, "toolbar_sidebar_and_dialog_unchanged": True,
            "annotation_controls_clickable": True, "global_across_tabs": True, "native_file_overrides_stale_cache": True,
            "restored_after_native_restart": 145,
            "reset_to_default": True, "source_unchanged": True}
        web.screenshot(evidence / "table-text-size.png")
    except Exception:
        try:
            report["table_text_failure"] = web.script("""
              return {menuOpen:document.querySelector('.workspace-menu')?.open,
                controls:[...document.querySelectorAll('[aria-label="Table text size"] button')].map(e=>{
                  const r=e.getBoundingClientRect(),hit=document.elementFromPoint((r.left+r.right)/2,(r.top+r.bottom)/2),s=getComputedStyle(e);
                  return {label:e.getAttribute('aria-label'),disabled:e.disabled,rect:[r.left,r.top,r.width,r.height],
                    display:s.display,visibility:s.visibility,hit:hit?{tag:hit.tagName,classes:String(hit.className)}:null};})};
            """)
            web.screenshot(evidence / "table-text-failure.png")
        except Exception:
            pass
        raise
    finally:
        if web.script("return !!document.querySelector('.memo-dialog');"): web.named("Cancel", ".memo-dialog")
        web.close_drawer()
        _size(web, None if web.script("return document.querySelector('[aria-label=\"Reset table text size\"]').disabled;") else "Reset", 100)
        open_project(web, data["name"])
        for path, project in [(source, owner), (other, second["meta"]["id"])]:
            if web.script("return !![...document.querySelectorAll('.tab-close')].find(e=>e.getAttribute('aria-label')===arguments[0]);", f"Close {path.name}"):
                web.named(f"Close {path.name}")
            web.command("delete_project", {"projectId": project}, request=True)
