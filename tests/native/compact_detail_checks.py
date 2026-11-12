"""Exercise dense row details with actual wheel and keyboard scrolling."""
from __future__ import annotations

import json
from pathlib import Path
import statistics
import uuid

from detail_timeline_checks import _copy_audit
from feedback_checks import _clear, _settle
from row_navigation import open_project, wait_for


def _geometry(web, last_id: str) -> dict:
    return web.script("""
      const dialog=document.querySelector('.row-detail-dialog'),s=document.querySelector('.row-detail-scroll'),
        d=dialog.getBoundingClientRect(),r=s.getBoundingClientRect(),last=s.querySelector('[data-column-id="'+arguments[0]+'"]'),a=last.getBoundingClientRect(),
        b=last.querySelector('[aria-label^="Filter in "]'),br=b.getBoundingClientRect(),list=s.querySelector('.row-fields').getBoundingClientRect();
      return {window:[innerWidth,innerHeight],dialog:{top:d.top,bottom:d.bottom,width:d.width,height:d.height},
        scroll:{top:s.scrollTop,height:s.clientHeight,total:s.scrollHeight,bottom:r.bottom},
        last:{top:a.top,bottom:a.bottom,visible:a.top>=r.top-1&&a.bottom<=r.bottom+1,
          clickable:b.contains(document.elementFromPoint(br.left+br.width/2,br.top+br.height/2))},
        list:{top:list.top,bottom:list.bottom,height:list.height},
        shortHeights:[...s.querySelectorAll('.row-detail-field')].filter(e=>e.querySelector('.row-field-label')?.textContent.trim().startsWith('field_')).map(e=>e.getBoundingClientRect().height)};
    """, last_id)


def _wheel(web, delta: int) -> None:
    point = web.script("const r=document.querySelector('.row-detail-scroll').getBoundingClientRect();return {x:Math.round(r.left+r.width*.7),y:Math.round(r.top+r.height*.5)};")
    web.request("POST", web.route("actions"), {"actions": [{"type": "wheel", "id": f"dense-row-detail-wheel-{uuid.uuid4().hex}", "actions": [
        {"type": "scroll", **point, "deltaX": 0, "deltaY": delta, "duration": 0}]}]})


def _key(web, value: str) -> None:
    web.request("POST", web.route("actions"), {"actions": [{"type": "key", "id": "dense-row-detail-keys", "actions": [
        {"type": "keyDown", "value": value}, {"type": "keyUp", "value": value}]}]})


def verify_compact_detail(web, data, root: Path, evidence: Path, report: dict, measure_only: bool = False) -> None:
    long_value = ('Original text with "quotes", commas, spaces and a newline\n' + 'x' * 220 + '\n') * 36
    last_value = "LAST-18446744073709551615"
    records = []
    for row in range(2):
        record = {"id": f"dense-row-{row}"}
        record.update({f"field_{index:02d}": f"short-{index:02d}" for index in range(64)})
        record["very_long"] = long_value if row == 0 else "ordinary"
        record["last_field"] = last_value if row == 0 else "other-last"
        records.append(record)
    source = root / f"dense-row-detail-{uuid.uuid4().hex[:8]}.jsonl"
    source.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")
    case = web.command("create_project", {"path": str(source)})
    project_id = case["meta"]["id"]
    checks = []
    start = len(web.trace())
    try:
        open_project(web, case["meta"]["name"])
        web.results(2)
        detail = web.command("load_project", {"projectId": project_id}, request=True)
        columns = {column["label"]: column["id"] for column in detail["columns_meta"]}
        for height in ((827,) if measure_only else (827, 620)):
            web.request("POST", web.route("window/rect"), {"width": 1280, "height": height})
            web.click(web.script("return document.querySelector('.data-row .stickyData');"))
            wait_for(lambda: web.script("return document.querySelectorAll('.row-detail-field').length;"), lambda count: count == len(columns), "all67 detail fields render")
            wait_for(lambda: web.script("return document.querySelector(arguments[0])?.querySelector('.row-field-value')?.textContent;", f'.row-detail-field[data-column-id="{columns["very_long"]}"]'), lambda value: value == long_value, "full long field arrives before scroll checks")
            _settle(web)
            before = _geometry(web, columns["last_field"])
            _wheel(web, 900)
            _settle(web)
            first_wheel = _geometry(web, columns["last_field"])
            if measure_only:
                checks.append({"before": before, "after_first_wheel": first_wheel})
                web.screenshot(evidence / "dense-detail-before-fix.png")
                break
            assert before["dialog"]["top"] >= -1 and before["dialog"]["bottom"] <= before["window"][1] + 1, before
            assert before["scroll"]["height"] > 0 and before["scroll"]["total"] > before["scroll"]["height"], before
            assert len(before["shortHeights"]) == 64 and statistics.median(before["shortHeights"]) <= 36, before
            wait_for(lambda: _geometry(web, columns["last_field"]), lambda g: g["scroll"]["top"] > before["scroll"]["top"], "trusted wheel scrolls the row-detail region")
            for _ in range(18):
                current = _geometry(web, columns["last_field"])
                if current["last"]["visible"]:
                    break
                _wheel(web, 900)
                _settle(web)
            wheel_end = _geometry(web, columns["last_field"])
            assert wheel_end["last"]["visible"] and wheel_end["last"]["clickable"], wheel_end
            web.screenshot(evidence / f"dense-detail-wheel-{height}.png")
            scroll = web.element('[aria-label="Row fields"]')
            assert web.script("return arguments[0].tabIndex;", scroll) == 0
            web.script("arguments[0].focus();", scroll)
            _key(web, "\ue011")  # Home
            wait_for(lambda: _geometry(web, columns["last_field"]), lambda g: g["scroll"]["top"] <= 1, "Home returns to the first detail field")
            _key(web, "\ue00f")  # PageDown
            wait_for(lambda: _geometry(web, columns["last_field"]), lambda g: g["scroll"]["top"] > 0, "PageDown scrolls the modal rather than the table")
            _key(web, "\ue010")  # End
            _settle(web)
            keyboard_end = wait_for(lambda: _geometry(web, columns["last_field"]),
                lambda g: g["last"]["visible"] and g["last"]["clickable"], "End exposes a clickable last detail field")
            assert web.script("return [...document.querySelectorAll('.row-field-actions button')].every(e=>!e.textContent.trim());"), "Field actions are compact icons with accessible names"
            assert not web.script("return !!document.querySelector('.row-detail-dialog [aria-label^=\"Filter out \"]');")
            web.screenshot(evidence / f"dense-detail-last-{height}.png")
            checks.append({"height": height, "before": before, "wheel_end": wheel_end, "keyboard_end": keyboard_end})
            if height == 827:
                web.named("Close", ".row-detail-dialog")
        if not measure_only:
            _copy_audit(web)
            try:
                web.named("Copy very_long", ".row-detail-dialog")
                wait_for(lambda: web.script("return document.querySelector('.row-detail-copy-status').textContent;"), lambda text: "Copied very_long" in text, "the full long field is copied")
                values = web.script("return window.__detailCopyAudit.values;")
                assert values and all(value == long_value for value in values), [len(value) for value in values]
            finally:
                web.script("window.__detailCopyAudit?.restore();delete window.__detailCopyAudit;")
            web.script("document.querySelector('[aria-label=\"Row fields\"]').focus();")
            _key(web, "\ue010")
            _settle(web)
            wait_for(lambda: _geometry(web, columns["last_field"]),
                lambda g: g["last"]["visible"] and g["last"]["clickable"], "last field actions remain reachable after copying")
            target = web.node('.row-detail-dialog [aria-label="Filter in last_field"]')
            web.tap(target)
            web.applied(projectId=project_id, condition={"column": columns["last_field"], "op": "eq", "values": [last_value], "negated": False})
            web.results(1)
            assert not web.script("return !!document.querySelector('.row-detail-dialog');")
            _clear(web, 2)
            assert not any(entry["name"] == "surrounding_rows" for entry in web.trace()[start:])
        report["compact_row_detail"] = {"fields": len(columns), "short_fields": 64, "long_characters": len(long_value),
            "measure_only": measure_only, "sizes": checks, "scroll_inputs": "Trusted WebDriver wheel, Home, PageDown and End; no assigned scroll offsets"}
    finally:
        if web.script("return !!document.querySelector('.row-detail-dialog');"):
            web.named("Close", ".row-detail-dialog")
        web.request("POST", web.route("window/rect"), {"width": 1280, "height": 827})
        web.close_drawer()
        web.tab(data["id"])
        web.results(data["total"])
        web.named(f"Close {source.name}")
        web.command("delete_project", {"projectId": project_id}, request=True)
        open_project(web, data["name"])
        web.results(data["total"])
