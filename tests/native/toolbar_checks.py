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


def verify_condition_buttons(web, evidence: Path, report: dict) -> None:
    web.drawer("Conditions")
    try:
        buttons = web.script("""
          const root=document.querySelector('.condition-editor .group-toolbar'),c=document.createElement('canvas').getContext('2d');
          return ['Add condition','Add group'].map(name=>{
            const e=root.querySelector('[aria-label="'+name+'"]'),r=e.getBoundingClientRect(),s=getComputedStyle(e);
            c.clearRect(0,0,1,1);c.fillStyle=s.borderTopColor;c.fillRect(0,0,1,1);
            return {name,tag:e.tagName,text:e.textContent.trim(),width:r.width,height:r.height,
              border:parseFloat(s.borderTopWidth),borderAlpha:c.getImageData(0,0,1,1).data[3],
              reachable:e.contains(document.elementFromPoint(r.left+r.width/2,r.top+r.height/2))};
          });
        """)
        assert all(button["tag"] == "BUTTON" and button["width"] >= 40
            and 24 <= button["height"] <= 28 and button["border"] >= 1
            and button["borderAlpha"] > 0 and button["reachable"] for button in buttons), buttons
        assert "Condition" in buttons[0]["text"] and "Group" in buttons[1]["text"], buttons
        web.script("document.querySelector('.condition-editor [aria-label=\"Add condition\"]').focus();")
        web.request("POST", web.route("actions"), {"actions": [{"type": "key", "id": "condition-button-focus", "actions": [
            {"type": "keyDown", "value": "\ue004"}, {"type": "keyUp", "value": "\ue004"},
            {"type": "keyDown", "value": "\ue008"}, {"type": "keyDown", "value": "\ue004"},
            {"type": "keyUp", "value": "\ue004"}, {"type": "keyUp", "value": "\ue008"}]}]})
        focus = web.script("const e=document.activeElement,s=getComputedStyle(e);return {name:e.getAttribute('aria-label'),visible:e.matches(':focus-visible'),outline:parseFloat(s.outlineWidth)};")
        assert focus["name"] == "Add condition" and focus["visible"] and focus["outline"] >= 2, focus
        web.screenshot(evidence / "condition-add-buttons.png")
        report["condition_buttons"] = {"buttons": buttons, "keyboard_focus": focus}
    finally:
        web.close_drawer()
