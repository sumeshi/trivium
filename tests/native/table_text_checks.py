"""Table text and row density preserve navigation, evidence and surrounding UI."""
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
SIZES = {55, 70, 85, 100, 115, 130, 145}
PAGE_SIZE = 250
PREFETCH_PAGES = 1


def _fit_failure(percent: int, index: int, control: dict) -> None:
    """Public diagnostics contain only fixed labels and bounded numeric geometry."""
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return
    safe = {"control": "tag" if control["tag"] else "flag", "index": index}
    if percent in SIZES:
        safe["percent"] = percent
    for key in ("width", "height", "contentWidth", "contentHeight", "advanceWidth", "inkWidth", "inkHeight", "fontSize", "leftClearance", "rightClearance"):
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

    desired = {"text": f"{expected}%", "disabled": [expected == min(SIZES), expected == 100, expected == max(SIZES)]}
    try:
        wait_for(read_controls, lambda controls: controls == desired, "table text controls update", timeout=8)
    except AssertionError as error:
        if os.environ.get("GITHUB_ACTIONS") == "true" and str(error).startswith("Timed out: table text controls update;"):
            observed = next((size for size in SIZES if (last_controls or {}).get("text") == f"{size}%"), None)
            safe = {"action": action if action in {"Decrease", "Increase", "Reset"} else "read",
                    "expected": expected if expected in SIZES else None, "observed": observed}
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
            style=getComputedStyle(label),controlStyle=getComputedStyle(e),content=contentBox(e),labelWidth=contentBox(label).width;
          // clientWidth rounds fractional glyph widths; compare the same float CSS-pixel units.
          context.font=canvasFont(style);const m=context.measureText(label.textContent.trim()),
            inkWidth=m.actualBoundingBoxLeft+m.actualBoundingBoxRight,inkHeight=m.actualBoundingBoxAscent+m.actualBoundingBoxDescent;
          // A count may overhang its advance box into the button's padding.
          // Check its painted bounds, including clearance from the tag icon.
          const pen=tag?label.getBoundingClientRect().left+(labelWidth-m.width)/2:
            r.left+parseFloat(controlStyle.borderLeftWidth)+parseFloat(controlStyle.paddingLeft)+(content.width-m.width)/2,
            left=tag?e.querySelector('svg').getBoundingClientRect().right:r.left+parseFloat(controlStyle.borderLeftWidth),
            right=r.right-parseFloat(controlStyle.borderRightWidth),
            leftClearance=pen-m.actualBoundingBoxLeft-left,rightClearance=right-pen-m.actualBoundingBoxRight;
          return {height:r.height,width:r.width,tag,fontSize:parseFloat(style.fontSize),contentWidth:content.width,
            contentHeight:content.height,advanceWidth:m.width,inkWidth,inkHeight,
            leftClearance,rightClearance,
            fits:m.width<=content.width+.1&&leftClearance>=-.1&&rightClearance>=-.1&&inkHeight<=content.height};}),
        sevenDigitWidth,
        indexContentWidth:contentBox(index).width};
    """)
    for kind, base in BASE.items():
        assert abs(sample["fonts"][kind] - base * percent / 100) <= .03, (percent, kind, sample)
    row_height = round(32 * percent / 100)
    assert all(height == row_height for height in sample["rowHeights"]) and sample["headerHeight"] == 38, sample
    assert sample["sevenDigitWidth"] <= sample["indexContentWidth"] + .1, sample
    failed = [(index, control) for index, control in enumerate(sample["controls"])
              if not control["fits"] or control["height"] != min(20 if control["tag"] else 21, row_height - 4)]
    if failed:
        _fit_failure(percent, *failed[0])
    assert not failed, sample
    return sample


def _check_density_requests(query: dict, entries: list[dict], rendered_pages: set[int], loaded_pages: set[int], total: int) -> None:
    # The normal loader also fetches one neighboring page. Historical responses
    # cannot prove cache membership after annotation refresh or page eviction.
    maximum = (max(total, 1) - 1) // PAGE_SIZE
    allowed = set(range(max(0, min(rendered_pages) - PREFETCH_PAGES), min(maximum, max(rendered_pages) + PREFETCH_PAGES) + 1))
    seen = set(loaded_pages)
    for entry in entries:
        assert entry["name"] != "locate_project_row", "Density changes must preserve the source-row anchor without a lookup"
        if entry["name"] != "query_project_rows":
            continue
        request = entry["request"]
        assert all(request.get(key) == value for key, value in query.items()), entry
        assert request["offset"] % PAGE_SIZE == 0 and request["limit"] == PAGE_SIZE, entry
        page = request["offset"] // PAGE_SIZE
        assert page in allowed, "Density changes may only load rendered pages or their bounded prefetch neighbors"
        assert page not in seen, "Density changes must not reload a rendered page or request the same page twice"
        seen.add(page)


def _change(web, total: int, action: str, percent: int, baseline: dict) -> dict:
    before, query = web.state(), web.query()
    start = web.script("return performance.now();")
    _size(web, action, percent)
    after, sample = web.results(total), _sample(web, percent)
    assert sample["grid"] == baseline["grid"] and sample["outside"] == baseline["outside"], sample
    assert web.query() == query and after["selectionTotal"] == before["selectionTotal"]
    visible = lambda state: [row for row in state["rows"] if row["bottom"] > state["top"] + .5 and row["top"] < state["bottom"] - .5]
    assert visible(after)[0]["id"] == visible(before)[0]["id"], (before, after)
    assert after["scrollLeft"] == before["scrollLeft"]
    if action == "Decrease":
        assert len(visible(after)) > len(visible(before)), (before, after)
    ids = [row["id"] for row in after["rows"]]
    assert len(ids) == len(set(ids)) and all(right - left == 2 for left, right in zip(ids, ids[1:])), after
    assert all(abs(right["top"] - left["bottom"]) <= 1 for left, right in zip(after["rows"], after["rows"][1:])), after
    pages = lambda state: {(row["id"] // 2 - 1) // PAGE_SIZE for row in state["rows"]}
    _check_density_requests(query, [entry for entry in web.trace() if entry["started"] >= start], pages(after), pages(before), total)
    return {"percent": percent, "row_height": sample["rowHeights"][0], "rendered_rows": len(ids),
            "scroll_top": after["scrollTop"], "scroll_left": after["scrollLeft"], "fonts": sample["fonts"]}


def _target(web, total: int, source_row: int) -> None:
    wait_for(web.state, lambda state: state.get("ready") and state["total"] == total
             and all(row["id"] is not None for row in state["rows"])
             and any(row["id"] == source_row and row["top"] >= state["top"] - 1 and row["bottom"] <= state["bottom"] + 1
                     for row in state["rows"]), f"original #{source_row} is visible at the current density")
    web.results(total)


def _dialog(web) -> dict:
    web.tap(web.node('.data-row[data-row-index="1"] .memo-button'))
    wait_for(lambda: web.script("return !!document.querySelector('.memo-dialog');"), bool, "note editor opens at this text size")
    _settle(web)
    return web.script("const e=document.querySelector('.memo-dialog'),r=e.getBoundingClientRect();return {width:r.width,height:r.height,font:getComputedStyle(e).fontSize,inputFont:getComputedStyle(e.querySelector('textarea')).fontSize};")


def verify_table_text(app, data, root: Path, evidence: Path, report: dict) -> None:
    web = app.web
    resolved = web.async_script("""
      const done=arguments[arguments.length-1];
      window.__TAURI_INTERNALS__.invoke('plugin:path|resolve_directory',{directory:15})
        .then(path=>done({path}),()=>done({error:true}));
    """)
    assert isinstance(resolved.get("path"), str), "The native preference directory must resolve"
    directory = Path(resolved["path"])
    assert directory.is_absolute(), "The native preference directory must be absolute"
    directory = directory.resolve()
    if os.name == "nt":
        assert directory.name == "com.trivium.native-tests", "Windows checks must use the dedicated native-test namespace"
    else:
        assert directory.is_relative_to(root.resolve()), "The native preference must stay inside the isolated Linux profile"
    preference = directory / "table-text-size.json"

    def saved_size():
        try:
            return json.loads(preference.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

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
        reset = not web.script("return document.querySelector('[aria-label=\"Reset table text size\"]').disabled;")
        _size(web, "Reset" if reset else None, 100)
        if reset:
            wait_for(saved_size, lambda size: size == 100, "the previous native preference resets to 100%")
        baseline, dialog = _sample(web, 100), _dialog(web)
        web.named("Cancel", ".memo-dialog")
        web.drawer("Conditions")
        pane_script = "const e=document.querySelector('#analysis-drawer'),r=e.getBoundingClientRect();return {font:getComputedStyle(e.querySelector('h3')).fontSize,width:r.width,height:r.height};"
        pane = web.script(pane_script)
        web.close_drawer()
        web.click(web.node('input[aria-label="Select row 2"]'))
        checks = [_change(web, 300, "Decrease", percent, baseline) for percent in (85, 70, 55)]
        checks.extend(_change(web, 300, "Increase", percent, baseline) for percent in (70, 85, 100))
        web.go(300, 300, 2); _settle(web)
        checks.append(_change(web, 300, "Increase", 115, baseline))
        web.button("Last"); _target(web, 300, 600); _settle(web)
        checks.append(_change(web, 300, "Increase", 130, baseline))
        web.button("First"); _target(web, 300, 2); _settle(web)
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
        wait_for(saved_size, lambda size: size == 145, "145% is saved in the native preference file before restart")
        web.script("localStorage.setItem('trivium:table-text-size','130');")
        app.end_session(); web = app.start()
        _size(web, None, 145)
        assert web.script("return localStorage.getItem('trivium:table-text-size');") == "145"
        open_project(web, case["meta"]["name"]); web.results(300)
        _sample(web, 145)
        _size(web, "Reset", 100); _sample(web, 100)
        wait_for(saved_size, lambda size: size == 100, "reset to 100% is saved in the native preference file")
        assert source.read_bytes() == original
        report["table_text"] = {"sizes_and_positions": checks, "default_fonts": baseline["fonts"], "default_row_height": 32,
            "header_height": 38, "seven_digit_row_number_fits": True, "toolbar_sidebar_and_dialog_unchanged": True,
            "annotation_controls_clickable": True, "global_across_tabs": True, "native_file_overrides_stale_cache": True,
            "restored_after_native_restart": 145,
            "reset_to_default": True, "native_reset_saved": True, "source_unchanged": True}
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
