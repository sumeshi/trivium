"""Keep native scrolling covered by loaded rows and retained stripes stable."""
from __future__ import annotations

from pathlib import Path
import uuid

from feedback_checks import _settle
from row_navigation import wait_for


_ROWS = """
const canvas=document.createElement('canvas');canvas.width=canvas.height=1;const c=canvas.getContext('2d');
const alpha=value=>{c.clearRect(0,0,1,1);c.fillStyle=value;c.fillRect(0,0,1,1);return c.getImageData(0,0,1,1).data[3];};
return [19,20].map(id=>{const row=[...document.querySelectorAll('.data-row')].find(e=>e.querySelector('.cell.index')?.textContent.trim()===String(id));
 const bg=getComputedStyle(row).backgroundColor;
 return {id,background:bg,alpha:alpha(bg),pinned:[...row.querySelectorAll(':scope>.sticky')].map(e=>({background:getComputedStyle(e).backgroundColor,alpha:alpha(getComputedStyle(e).backgroundColor)}))};});
"""


def verify_scroll_rendering(web, data, evidence: Path, report: dict) -> None:
    web.close_drawer()
    web.results(data["total"])
    samples = []
    try:
        web.button("First")
        for delta in (256, 32):
            before = web.state()["scrollTop"]
            point = web.script("const r=document.querySelector('.virtual-viewport').getBoundingClientRect();return {x:Math.round(r.left+r.width*.7),y:Math.round(r.top+80)};")
            web.request("POST", web.route("actions"), {"actions": [{"type": "wheel", "id": f"stripe-wheel-{uuid.uuid4().hex}", "actions": [
                {"type": "scroll", **point, "deltaX": 0, "deltaY": delta, "duration": 0}]}]})
            wait_for(web.state, lambda value: abs(value["scrollTop"] - before - delta) <= 1,
                "trusted vertical wheel shifts the virtual window")
            _settle(web)
            first_rendered = web.script("return Number(document.querySelector('.data-row .cell.index')?.textContent);")
            rows = web.script(_ROWS)
            assert rows[0]["background"] != rows[1]["background"], rows
            for row in rows:
                assert row["alpha"] == 255 and all(cell["alpha"] == 255 and cell["background"] == row["background"] for cell in row["pinned"]), row
            samples.append({"wheel_delta": delta, "first_rendered": first_rendered, "retained_rows": rows})
        assert samples[1]["first_rendered"] == samples[0]["first_rendered"] + 1, samples
        assert samples[0]["retained_rows"] == samples[1]["retained_rows"], samples
        web.screenshot(evidence / "scroll-stable-row-stripes.png")
        web.button("First")
        web.results(data["total"])
        _settle(web)
        start = web.checkpoint()
        web.script("""
          const e=document.querySelector('.virtual-viewport'),p=window.__scrollTurnCoverage={e,samples:[]};
          p.listen=()=>queueMicrotask(()=>{
            const r=e.getBoundingClientRect(),rows=[...e.querySelectorAll('.data-row')],first=rows[0],last=rows.at(-1);
            const a=first?.getBoundingClientRect(),b=last?.getBoundingClientRect();
            const unloaded=rows.filter(row=>!row.hasAttribute('data-row-index')).filter(row=>{
              const box=row.getBoundingClientRect();return box.bottom>r.top+1&&box.top<r.bottom-1;}).length;
            p.samples.push({top:e.scrollTop,first:Number(first?.dataset.position),last:Number(last?.dataset.position),
              covered:!!a&&!!b&&a.top<=r.top+1&&(b.bottom>=r.bottom-1||Number(last.dataset.position)===arguments[0]-1),
              unloaded,loading:e.querySelectorAll('.data-row.loading').length});
          });e.addEventListener('scroll',p.listen,{passive:true});
        """, data["total"])
        turns = []
        for delta in (320, -320):
            before = web.state()["scrollTop"]
            count = web.script("return window.__scrollTurnCoverage.samples.length;")
            web.request("POST", web.route("actions"), {"actions": [{"type": "wheel", "id": f"turn-wheel-{uuid.uuid4().hex}", "actions": [
                {"type": "scroll", **point, "deltaX": 0, "deltaY": delta, "duration": 0}]}]})
            wait_for(web.state, lambda value: abs(value["scrollTop"] - before - delta) <= 1, "native wheel moves the cache-warm viewport")
            current = wait_for(lambda: web.script("return window.__scrollTurnCoverage.samples.slice(arguments[0]);", count),
                               bool, "scroll-event microtask records DOM coverage")
            assert all(item["covered"] and item["unloaded"] == 0 and item["loading"] == 0 for item in current), current
            turns.extend(current)
        assert not any(entry["name"] == "query_project_rows" for entry in web.trace(after=start)), "The coverage regression must use already cached rows"
        report["stable_scroll_rendering"] = {"retained_rows_keep_original_stripe_across_virtual_window_parity": True,
            "samples": samples, "loaded_rows_cover_viewport_in_scroll_event_turn": turns}
    finally:
        web.script("const p=window.__scrollTurnCoverage;if(p){p.e.removeEventListener('scroll',p.listen);delete window.__scrollTurnCoverage;}")
        web.button("First")
        web.results(data["total"])
