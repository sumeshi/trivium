"""Keep the investigation toolbar compact without losing narrow-screen access."""
from __future__ import annotations

from pathlib import Path

from feedback_checks import _settle
from row_navigation import wait_for


def _geometry(web) -> dict:
    return web.script("""
      const controls=[
        ['Back','.history-buttons [aria-label="Back"]'],
        ['Forward','.history-buttons [aria-label="Forward"]'],
        ['Bookmark','.bookmark-toggle'],['Flag','.flag-trigger'],
        ['Search','.search-controls'],['Columns','.column-picker>button'],
        ['Original row','.row-jump input'],['First','.row-jump [aria-label="First"]'],
        ['Last','.row-jump [aria-label="Last"]'],['Filters','#tool-group-filters'],
        ['Analysis','#tool-group-analysis'],['Annotations','#tool-group-annotations'],
        ['Export','.export-button']
      ];
      return {width:innerWidth,overflow:document.documentElement.scrollWidth>innerWidth,
        goButton:!!document.querySelector('.row-jump [aria-label="Go"]'),
        controls:controls.map(([name,selector])=>{
          const e=document.querySelector(selector),r=e.getBoundingClientRect();
          return {name,left:r.left,right:r.right,top:r.top,bottom:r.bottom,center:(r.top+r.bottom)/2};
        })};
    """)


def verify_toolbar(web, data, evidence: Path, report: dict) -> None:
    web.close_drawer()
    checks = []
    try:
        for width in [1280, 800, 320]:
            web.request("POST", web.route("window/rect"), {"width": width, "height": 827})
            _settle(web)
            state = _geometry(web)
            assert not state["overflow"] and not state["goButton"], state
            assert all(item["left"] >= -1 and item["right"] <= state["width"] + 1
                and item["bottom"] > item["top"] for item in state["controls"]), state
            if width == 1280:
                centers = [item["center"] for item in state["controls"]]
                assert max(centers) - min(centers) <= 1, state
                assert all(right["left"] >= left["right"] - 1
                    for left, right in zip(state["controls"], state["controls"][1:])), state
            elif width == 800:
                primary, secondary = state["controls"][:6], state["controls"][6:]
                assert max(item["center"] for item in primary) - min(item["center"] for item in primary) <= 1, state
                assert max(item["center"] for item in secondary) - min(item["center"] for item in secondary) <= 1, state
                assert min(item["top"] for item in secondary) >= max(item["bottom"] for item in primary), state
            else:
                web.tap(web.node('.flag-trigger'))
                menu = web.script("const r=document.querySelector('.flag-menu').getBoundingClientRect();return {left:r.left,right:r.right};")
                assert menu["left"] >= -1 and menu["right"] <= state["width"] + 1, menu
                web.tap(web.node('.flag-trigger'))
                state["flag_menu"] = menu
            web.screenshot(evidence / f"toolbar-{width}.png")
            checks.append(state)
        web.request("POST", web.route("window/rect"), {"width": 1280, "height": 827})
        web.input('.row-jump input[type="number"]', "242", enter=True)
        wait_for(web.state, lambda state: any(row["id"] == 242 and row["top"] >= state["top"] - 1
            and row["bottom"] <= state["bottom"] + 1 for row in state.get("rows", [])), "Enter jumps to the original row")
        web.button("Last")
        wait_for(web.state, lambda state: any(row["id"] == data["total"] for row in state.get("rows", [])), "Last remains accessible")
        report["toolbar"] = {"geometry": checks, "enter_jumps_to_original_row": True}
    finally:
        web.request("POST", web.route("window/rect"), {"width": 1280, "height": 827})
        web.button("First")
        web.results(data["total"])
